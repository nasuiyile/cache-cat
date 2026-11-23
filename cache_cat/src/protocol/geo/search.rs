//! Search for the members of a GEO index around a position, shared by the
//! commands that query by distance.
//!
//! This follows Redis step by step. `geohash_helper.c` picks up to nine cells
//! that cover the search area, and `geo.c` scans the score range of each cell
//! in a fixed order and keeps the members inside the area. Which cells are
//! scanned, and in which order, decides the order of an unsorted reply and
//! the members a `COUNT ... ANY` search stops at, so none of it is simplified.

use crate::protocol::geo::codec::{GeoHashBits, decode_score};
use crate::protocol::geo::distance::{
    DEGREES_TO_RADIANS, EARTH_RADIUS_IN_METERS, GeoUnit, distance,
};
use crate::raft::types::core::structure::sorted_set::SortedSet;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Redis `MERCATOR_MAX`: half the length of the equator in meters, the size
/// of a cell that spans a whole coordinate range.
const MERCATOR_MAX: f64 = 20037726.37;

/// The cell of the centre and its neighbours as `(east, north)` offsets, in
/// the order Redis scans them: centre, north, south, east, west, north-east,
/// north-west, south-east, south-west.
const SCAN_ORDER: [(i8, i8); 9] = [
    (0, 0),
    (0, 1),
    (0, -1),
    (1, 0),
    (-1, 0),
    (1, 1),
    (-1, 1),
    (1, -1),
    (-1, -1),
];

/// Order of the members a search returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeoOrder {
    /// Nearest first.
    Asc,
    /// Farthest first.
    Desc,
}

/// A member found by a search.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct GeoPoint {
    pub member: Bytes,
    /// Score of the member in the index.
    pub score: f64,
    /// Centre of the member's cell, the position GEOPOS reports.
    pub longitude: f64,
    pub latitude: f64,
    /// Meters between that position and the centre of the search.
    pub distance: f64,
}

/// A search for the members within `radius` of a position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadiusSearch {
    pub longitude: f64,
    pub latitude: f64,
    /// In `unit`, as the client sent it. Redis multiplies the two wherever it
    /// needs meters, and so does this search.
    pub radius: f64,
    pub unit: GeoUnit,
    /// `COUNT`: the most members to return.
    pub count: Option<u64>,
    /// `ANY`: stop at the first `count` members found instead of looking for
    /// the nearest ones.
    pub any: bool,
    pub order: Option<GeoOrder>,
}

impl fmt::Display for RadiusSearch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.longitude, self.latitude, self.radius, self.unit
        )?;
        if let Some(count) = self.count {
            write!(f, " COUNT {count}")?;
        }
        if self.any {
            f.write_str(" ANY")?;
        }
        match self.order {
            Some(GeoOrder::Asc) => f.write_str(" ASC"),
            Some(GeoOrder::Desc) => f.write_str(" DESC"),
            None => Ok(()),
        }
    }
}

/// What a scan takes out of an index while its lock is held.
pub(super) enum Scan {
    /// The members of the scanned cells, still to be measured.
    Candidates(Vec<(Bytes, f64)>),
    /// The members inside the circle that `ANY` stopped at.
    Points(Vec<GeoPoint>),
}

impl RadiusSearch {
    /// The members of `zset` inside the circle, ordered and limited like the
    /// result of Redis `georadiusGeneric`.
    ///
    /// Without an order they come as the scan finds them: cell by cell, and
    /// by score and member inside a cell. Members at the same distance keep
    /// that order when they are sorted, in either direction.
    pub(super) fn points(&self, zset: &Mutex<SortedSet>) -> Vec<GeoPoint> {
        // The lock is released when the scan returns.
        let scan = self.scan(&zset.lock());
        self.finish(scan)
    }

    /// The part of `points` that reads the index. A search whose centre comes
    /// from the index calls it under the lock that looked the centre up, and
    /// `finish` once that lock is released.
    pub(super) fn scan(&self, zset: &SortedSet) -> Scan {
        let radius_meters = self.radius_meters();
        let cells = search_cells(self.longitude, self.latitude, radius_meters);
        if !self.any {
            // Only the candidates are copied under the lock. Decoding and
            // trigonometry, the bulk of the work, are left to `finish`.
            return Scan::Candidates(
                score_ranges(cells)
                    .flat_map(|(min, max)| zset.score_range(min, max))
                    .map(|(member, score)| (member.clone(), score))
                    .collect(),
            );
        }

        // The scan ends at the first `limit` members inside the circle, so
        // they have to be measured while the lock is held.
        let limit = self.limit();
        let mut points = Vec::new();
        'cells: for (min, max) in score_ranges(cells) {
            for (member, score) in zset.score_range(min, max) {
                if let Some(position) = self.locate(radius_meters, score) {
                    points.push(GeoPoint::at(member.clone(), score, position));
                    if points.len() >= limit {
                        break 'cells;
                    }
                }
            }
        }
        Scan::Points(points)
    }

    /// The part of `points` that needs no lock: measures the candidates, then
    /// orders and limits the members inside the circle.
    pub(super) fn finish(&self, scan: Scan) -> Vec<GeoPoint> {
        let mut points = match scan {
            Scan::Points(points) => points,
            Scan::Candidates(candidates) => {
                let radius_meters = self.radius_meters();
                candidates
                    .into_iter()
                    .filter_map(|(member, score)| {
                        let position = self.locate(radius_meters, score)?;
                        Some(GeoPoint::at(member, score, position))
                    })
                    .collect()
            }
        };

        let returned = points.len().min(self.limit());
        // COUNT asks for the nearest members unless ANY takes what comes.
        let order = self
            .order
            .or((self.count.is_some() && !self.any).then_some(GeoOrder::Asc));
        match order {
            Some(order) => sort_by_distance(points, order, returned),
            None => {
                points.truncate(returned);
                points
            }
        }
    }

    fn radius_meters(&self) -> f64 {
        self.radius * self.unit.to_meters()
    }

    fn limit(&self) -> usize {
        self.count
            .map_or(usize::MAX, |count| count.try_into().unwrap_or(usize::MAX))
    }

    /// `geoWithinShape`: the position of a score and its distance from the
    /// centre, if that is within the radius. A member is measured from the
    /// centre of its cell.
    fn locate(&self, radius_meters: f64, score: f64) -> Option<(f64, f64, f64)> {
        let (longitude, latitude) = decode_score(score);
        let distance = distance(self.longitude, self.latitude, longitude, latitude);
        // Negated like the C test, which keeps a distance that is NaN.
        (!(distance > radius_meters)).then_some((longitude, latitude, distance))
    }
}

impl GeoPoint {
    fn at(member: Bytes, score: f64, (longitude, latitude, distance): (f64, f64, f64)) -> Self {
        GeoPoint {
            member,
            score,
            longitude,
            latitude,
            distance,
        }
    }
}

/// The first `returned` points by distance, sorted.
///
/// Redis sorts with `qsort`, or with a partial sort when COUNT cuts the
/// result, and neither defines the order of equal distances. Here they keep
/// the order of the scan in both directions, as a stable sort would.
fn sort_by_distance(points: Vec<GeoPoint>, order: GeoOrder, returned: usize) -> Vec<GeoPoint> {
    // Redis `sort_gp_asc`, completed to a total order: a NaN, which the C
    // comparison takes for the smaller side whichever side it is on, goes last.
    let compare = |a: &(f64, usize), b: &(f64, usize)| {
        let by_distance =
            a.0.partial_cmp(&b.0)
                .unwrap_or_else(|| a.0.is_nan().cmp(&b.0.is_nan()));
        match order {
            GeoOrder::Asc => by_distance,
            GeoOrder::Desc => by_distance.reverse(),
        }
        .then(a.1.cmp(&b.1))
    };

    // Sorting distances with their positions makes the order of the scan the
    // tie-break, and moves 16 bytes instead of whole points.
    let mut keys: Vec<(f64, usize)> = points
        .iter()
        .enumerate()
        .map(|(position, point)| (point.distance, position))
        .collect();
    if returned < keys.len() {
        // Selecting first keeps a small COUNT on a large result linear.
        keys.select_nth_unstable_by(returned, compare);
        keys.truncate(returned);
    }
    keys.sort_unstable_by(compare);

    let mut points: Vec<Option<GeoPoint>> = points.into_iter().map(Some).collect();
    keys.into_iter()
        .filter_map(|(_, position)| points[position].take())
        .collect()
}

/// `geohashEstimateStepsByRadius`: the bits per coordinate at which a cell is
/// about as large as the search area.
fn estimate_steps(range_meters: f64, latitude: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut range = range_meters;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    // Make sure the range is included in most of the base cases.
    step -= 2;
    // Cells cover less ground towards the poles, so use larger ones there.
    #[allow(clippy::manual_range_contains)]
    if latitude > 66.0 || latitude < -66.0 {
        step -= 1;
        if latitude > 80.0 || latitude < -80.0 {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// `geohashBoundingBox` for a circle: `[min longitude, min latitude,
/// max longitude, max latitude]` of a box around it.
fn bounding_box(longitude: f64, latitude: f64, radius_meters: f64) -> [f64; 4] {
    let degrees = |radians: f64| radians / DEGREES_TO_RADIANS;
    let lat_delta = degrees(radius_meters / EARTH_RADIUS_IN_METERS);
    // A degree of longitude is shortest at the edge nearer to the pole.
    let long_delta = |edge_latitude: f64| {
        degrees(
            radius_meters / EARTH_RADIUS_IN_METERS / libm::cos(edge_latitude * DEGREES_TO_RADIANS),
        )
    };
    let long_delta = if latitude < 0.0 {
        long_delta(latitude - lat_delta)
    } else {
        long_delta(latitude + lat_delta)
    };
    [
        longitude - long_delta,
        latitude - lat_delta,
        longitude + long_delta,
        latitude + lat_delta,
    ]
}

/// `geohashCalculateAreasByShapeWGS84` for a circle: the cells to scan, in
/// `SCAN_ORDER`. `None` is a neighbour the search area does not reach.
fn search_cells(
    longitude: f64,
    latitude: f64,
    radius_meters: f64,
) -> [Option<GeoHashBits>; SCAN_ORDER.len()] {
    let [min_lon, min_lat, max_lon, max_lat] = bounding_box(longitude, latitude, radius_meters);
    let mut steps = estimate_steps(radius_meters, latitude);
    let mut cell = GeoHashBits::of(longitude, latitude, steps);

    // Near an edge of the cell the neighbour on that side may still be too
    // small to hold the rest of the area. One step less doubles the cells.
    let north = cell.neighbor(0, 1).area();
    let south = cell.neighbor(0, -1).area();
    let east = cell.neighbor(1, 0).area();
    let west = cell.neighbor(-1, 0).area();
    let too_small = north.latitude.1 < max_lat
        || south.latitude.0 > min_lat
        || east.longitude.1 < max_lon
        || west.longitude.0 > min_lon;
    if steps > 1 && too_small {
        steps -= 1;
        cell = GeoHashBits::of(longitude, latitude, steps);
    }

    // Drop the neighbours on a side where the centre cell already holds the
    // whole area.
    let area = cell.area();
    let useless = |(east, north): (i8, i8)| {
        steps >= 2
            && ((north < 0 && area.latitude.0 < min_lat)
                || (north > 0 && area.latitude.1 > max_lat)
                || (east < 0 && area.longitude.0 < min_lon)
                || (east > 0 && area.longitude.1 > max_lon))
    };
    SCAN_ORDER.map(|offset| (!useless(offset)).then(|| cell.neighbor(offset.0, offset.1)))
}

/// `membersOfAllNeighbors` and `scoresOfGeoHashBox`: the score range
/// `[min, max)` of each cell to scan, in order.
///
/// With a huge radius adjacent neighbours can be the same cell. Redis skips a
/// cell equal to the one it scanned last, but tests the index of that cell
/// for zero first, so a cell is never compared with the centre cell.
fn score_ranges(
    cells: [Option<GeoHashBits>; SCAN_ORDER.len()],
) -> impl Iterator<Item = (f64, f64)> {
    let mut last_scanned = 0;
    (0..cells.len()).filter_map(move |index| {
        let cell = cells[index]?;
        if last_scanned != 0 && cells[last_scanned] == Some(cell) {
            return None;
        }
        last_scanned = index;
        // Every score that starts with the bits of the cell: the next cell
        // number, aligned the same way, is the first score outside.
        let unused = 52 - u32::from(cell.step) * 2;
        Some((
            (cell.bits << unused) as f64,
            (cell.bits.wrapping_add(1) << unused) as f64,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::geo::codec::{
        GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, encode_score,
    };
    use crate::protocol::geo::distance::distance_reply;
    use crate::raft::types::core::response_value::Value;

    fn index(members: &[(&str, f64, f64)]) -> Mutex<SortedSet> {
        let mut zset = SortedSet::new();
        for (member, longitude, latitude) in members {
            zset.zincrby(
                Bytes::copy_from_slice(member.as_bytes()),
                encode_score(*longitude, *latitude),
            );
        }
        Mutex::new(zset)
    }

    fn search(longitude: f64, latitude: f64, radius: f64, unit: GeoUnit) -> RadiusSearch {
        RadiusSearch {
            longitude,
            latitude,
            radius,
            unit,
            count: None,
            any: false,
            order: None,
        }
    }

    fn names(points: &[GeoPoint]) -> Vec<&str> {
        points
            .iter()
            .map(|point| std::str::from_utf8(&point.member).unwrap())
            .collect()
    }

    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `[0, 1)`.
    fn unit_interval(state: &mut u64) -> f64 {
        (splitmix(state) >> 11) as f64 / (1u64 << 53) as f64
    }

    #[test]
    fn redis_documentation_example_finds_both_cities_in_score_order() {
        let sicily = index(&[
            ("Catania", 15.087269, 37.502669),
            ("Palermo", 13.361389, 38.115556),
        ]);
        let query = search(15.0, 37.0, 200.0, GeoUnit::Kilometers);
        let points = query.points(&sicily);
        // Palermo has the lower score, whatever the order of insertion.
        assert_eq!(names(&points), ["Palermo", "Catania"]);
        let kilometers = |point: &GeoPoint| match distance_reply(point.distance / 1000.0) {
            Value::BulkString(Some(text)) => String::from_utf8(text.to_vec()).unwrap(),
            other => panic!("{other:?}"),
        };
        assert_eq!(kilometers(&points[0]), "190.4424");
        assert_eq!(kilometers(&points[1]), "56.4413");
        assert_eq!(points[0].score, 3479099956230698.0);
        assert_eq!(
            (points[1].longitude, points[1].latitude),
            (15.087267458438873, 37.50266842333162)
        );

        // 100 km reach Catania only, and the units convert the radius.
        for (radius, unit) in [
            (100.0, GeoUnit::Kilometers),
            (100_000.0, GeoUnit::Meters),
            (62.2, GeoUnit::Miles),
            (328_084.0, GeoUnit::Feet),
        ] {
            let points = search(15.0, 37.0, radius, unit).points(&sicily);
            assert_eq!(names(&points), ["Catania"], "{radius} {unit}");
        }
        assert!(
            search(15.0, 37.0, 56.0, GeoUnit::Kilometers)
                .points(&sicily)
                .is_empty()
        );
    }

    #[test]
    fn step_estimates_follow_the_radius_and_the_latitude() {
        for (meters, latitude, steps) in [
            (0.0, 0.0, 26),
            // 2^25 meters is the first doubling past MERCATOR_MAX.
            (1.0, 0.0, 24),
            (200_000.0, 37.0, 6),
            (200_000.0, 66.0, 6),
            (200_000.0, 66.5, 5),
            (200_000.0, -70.0, 5),
            (200_000.0, 80.5, 4),
            (200_000.0, -85.0, 4),
            (MERCATOR_MAX, 0.0, 1),
            (1e300, 0.0, 1),
            (f64::INFINITY, 85.0, 1),
            // A tiny radius is clamped to the precision of a score.
            (1e-9, 0.0, 26),
            (5e-324, 85.0, 26),
        ] {
            assert_eq!(
                estimate_steps(meters, latitude),
                steps,
                "{meters} at {latitude}"
            );
        }
    }

    #[test]
    fn only_the_cells_the_circle_reaches_are_scanned() {
        // A cell of a score is about 0.5 by 0.3 meters at this latitude, so
        // five centimeters around its centre stay inside it.
        let score = encode_score(15.0, 37.0);
        let cell = GeoHashBits {
            bits: score as u64,
            step: 26,
        };
        let (longitude, latitude) = decode_score(score);
        let cells = search_cells(longitude, latitude, 0.05);
        assert_eq!(cells[0], Some(cell));
        assert!(cells[1..].iter().all(Option::is_none), "{cells:?}");
        assert_eq!(
            score_ranges(cells).collect::<Vec<_>>(),
            [(score, score + 1.0)]
        );

        // From the south-west corner of the cell the circle also reaches
        // the cells to the south, to the west and between them.
        let area = cell.area();
        let cells = search_cells(area.longitude.0 + 1e-8, area.latitude.0 + 1e-8, 0.05);
        assert_eq!(
            cells,
            [
                Some(cell),
                None,
                Some(cell.neighbor(0, -1)),
                None,
                Some(cell.neighbor(-1, 0)),
                None,
                None,
                None,
                Some(cell.neighbor(-1, -1)),
            ]
        );
        assert_eq!(score_ranges(cells).count(), 4);
    }

    #[test]
    fn a_huge_radius_scans_every_cell_once() {
        // At one step the grid has four cells: the two neighbours on each
        // axis are the same cell, and so are all four diagonal ones.
        let cells = search_cells(15.0, 37.0, 30_000_000.0);
        assert!(cells.iter().all(|cell| cell.is_some_and(|c| c.step == 1)));
        assert_eq!(cells[1], cells[2]);
        assert_eq!(cells[3], cells[4]);
        assert_eq!(cells[5], cells[8]);
        let mut ranges: Vec<_> = score_ranges(cells).collect();
        assert_eq!(ranges.len(), 4);
        ranges.sort_by(|a, b| a.0.total_cmp(&b.0));
        let quarter = (1u64 << 50) as f64;
        for (index, range) in ranges.iter().enumerate() {
            assert_eq!(
                *range,
                (index as f64 * quarter, (index + 1) as f64 * quarter)
            );
        }

        // So every member of the index is found exactly once.
        let world = index(&[
            ("north-east", 100.0, 60.0),
            ("north-west", -100.0, 60.0),
            ("south-east", 100.0, -60.0),
            ("south-west", -100.0, -60.0),
            ("origin", 0.0, 0.0),
        ]);
        let mut found = names(&search(15.0, 37.0, 30_000.0, GeoUnit::Kilometers).points(&world))
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        found.sort();
        assert_eq!(
            found,
            [
                "north-east",
                "north-west",
                "origin",
                "south-east",
                "south-west"
            ]
        );
    }

    #[test]
    fn every_member_inside_the_circle_is_found_in_scan_order() {
        let mut state = 20261121;
        for round in 0..400 {
            let longitude = GEO_LONG_MIN + unit_interval(&mut state) * 360.0;
            let latitude = -60.0 + unit_interval(&mut state) * 120.0;
            // From one meter to a thousand kilometers.
            let radius = 10f64.powf(unit_interval(&mut state) * 6.0);
            let reach = radius / 111_000.0;

            // Half the members lie around the circle, the rest anywhere.
            let mut zset = SortedSet::new();
            for member in 0..300 {
                let (member_longitude, member_latitude) = if member % 2 == 0 {
                    let east = (unit_interval(&mut state) - 0.5) * 6.0 * reach;
                    let north = (unit_interval(&mut state) - 0.5) * 4.0 * reach;
                    // Wrap around the antimeridian like the grid does.
                    let wrapped = (longitude + east + 540.0).rem_euclid(360.0) - 180.0;
                    (wrapped, (latitude + north).clamp(GEO_LAT_MIN, GEO_LAT_MAX))
                } else {
                    (
                        GEO_LONG_MIN + unit_interval(&mut state) * 360.0,
                        GEO_LAT_MIN + unit_interval(&mut state) * 2.0 * GEO_LAT_MAX,
                    )
                };
                zset.zincrby(
                    Bytes::from(format!("m{member}")),
                    encode_score(member_longitude, member_latitude),
                );
            }
            let members = zset.zrange(0, -1);
            let zset = Mutex::new(zset);

            let query = search(longitude, latitude, radius, GeoUnit::Meters);
            let found = query.points(&zset);
            let mut expected: Vec<Bytes> = members
                .iter()
                .filter(|(_, score)| {
                    let (member_longitude, member_latitude) = decode_score(*score);
                    distance(longitude, latitude, member_longitude, member_latitude) <= radius
                })
                .map(|(member, _)| member.clone())
                .collect();
            let mut actual: Vec<Bytes> = found.iter().map(|point| point.member.clone()).collect();
            actual.sort();
            expected.sort();
            assert_eq!(
                actual, expected,
                "round {round}: {radius} m around {longitude},{latitude}"
            );
            for point in &found {
                assert!(point.distance <= radius);
                assert_eq!((point.longitude, point.latitude), decode_score(point.score));
            }

            // ANY returns the first members of that same scan.
            let any = RadiusSearch {
                count: Some(3),
                any: true,
                ..query.clone()
            };
            let first = any.points(&zset);
            assert_eq!(first[..], found[..found.len().min(3)], "round {round}");

            // COUNT returns the nearest ones, nearest first.
            let nearest = RadiusSearch {
                count: Some(3),
                ..query.clone()
            }
            .points(&zset);
            let mut sorted = found.clone();
            sorted.sort_by(|a, b| a.distance.total_cmp(&b.distance));
            sorted.truncate(3);
            assert_eq!(nearest, sorted, "round {round}");
        }
    }

    #[test]
    fn the_circle_reaches_across_the_antimeridian_and_the_limits() {
        let edge = index(&[
            ("west", -179.9, 0.0),
            ("east", 179.9, 0.0),
            ("limit", GEO_LONG_MAX, 0.0),
            ("far", 0.0, 0.0),
            ("top", 0.0, GEO_LAT_MAX),
        ]);
        let mut found: Vec<String> =
            names(&search(179.95, 0.0, 30.0, GeoUnit::Kilometers).points(&edge))
                .into_iter()
                .map(str::to_owned)
                .collect();
        found.sort();
        // GEOADD gives longitude 180 a cell number one past the grid, which
        // no cell of a search centred anywhere else covers, in Redis either.
        assert_eq!(found, ["east", "west"]);

        // A search centred on the upper limits has such a cell number too,
        // and finds the member GEOADD stored there.
        let corner = index(&[("corner", GEO_LONG_MAX, GEO_LAT_MAX)]);
        let points = search(GEO_LONG_MAX, GEO_LAT_MAX, 1.0, GeoUnit::Meters).points(&corner);
        assert_eq!(names(&points), ["corner"]);
        assert_eq!(points[0].distance, 0.0);
    }

    #[test]
    fn sorting_and_count_keep_the_scan_order_of_equal_distances() {
        // Two members share a cell; the others lie at growing distances.
        let line = index(&[
            ("b", 10.0, 10.0),
            ("a", 10.0, 10.0),
            ("near", 10.001, 10.0),
            ("mid", 10.01, 10.0),
            ("far", 10.1, 10.0),
        ]);
        let query = search(10.0, 10.0, 100.0, GeoUnit::Kilometers);
        let scan = names(&query.points(&line))
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        // Equal scores come in member order.
        let a = scan.iter().position(|name| name == "a").unwrap();
        assert_eq!(scan[a + 1], "b");

        let ordered = |order: Option<GeoOrder>, count: Option<u64>, any: bool| {
            let points = RadiusSearch {
                order,
                count,
                any,
                ..query.clone()
            }
            .points(&line);
            names(&points)
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ordered(Some(GeoOrder::Asc), None, false),
            ["a", "b", "near", "mid", "far"]
        );
        assert_eq!(
            ordered(Some(GeoOrder::Desc), None, false),
            ["far", "mid", "near", "a", "b"]
        );
        // COUNT alone sorts ascending, and cuts a tie after its first member.
        assert_eq!(ordered(None, Some(1), false), ["a"]);
        assert_eq!(ordered(None, Some(3), false), ["a", "b", "near"]);
        assert_eq!(
            ordered(Some(GeoOrder::Desc), Some(4), false),
            ["far", "mid", "near", "a"]
        );
        assert_eq!(
            ordered(None, Some(100), false),
            ["a", "b", "near", "mid", "far"]
        );
        // ANY keeps the order of the scan unless an order is given, which
        // then sorts only the members the scan stopped at.
        assert_eq!(ordered(None, Some(2), true), scan[..2]);
        assert_eq!(ordered(None, Some(100), true), scan);
        let mut first_three = scan[..3].to_vec();
        let far_first = |name: &String| match name.as_str() {
            "far" => 0,
            "mid" => 1,
            "near" => 2,
            "a" => 3,
            _ => 4,
        };
        first_three.sort_by_key(far_first);
        assert_eq!(ordered(Some(GeoOrder::Desc), Some(3), true), first_three);
    }
}

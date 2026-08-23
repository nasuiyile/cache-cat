use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;

use async_trait::async_trait;
use bytes::Bytes;
use mimalloc::MiMalloc;
use serde::Deserialize;

const MIB: u64 = 1024 * 1024;

/*
 * Redis MEMORY DOCTOR:
 *
 * 如果内存使用小于约 5 MiB，
 * 认为样本太小，不适合进行 memory diagnosis。
 */
const MIN_MEMORY_FOR_DIAGNOSIS: u64 = 5 * MIB;

/*
 * Redis 原版判断 memory peak 的阈值：
 *
 * peak > current * 1.5
 */
const HIGH_PEAK_RATIO: f64 = 1.5;

#[derive(Debug, Default, Deserialize)]
struct MiStatCount {
    #[serde(default)]
    total: i64,

    #[serde(default)]
    peak: i64,

    #[serde(default)]
    current: i64,
}

#[derive(Debug, Default, Deserialize)]
struct MiProcessStats {
    #[serde(default)]
    rss_current: u64,

    #[serde(default)]
    rss_peak: u64,

    #[serde(default)]
    commit_current: u64,

    #[serde(default)]
    commit_peak: u64,

    #[serde(default)]
    page_faults: u64,
}

#[derive(Debug, Deserialize)]
struct MiMallocStats {
    process: MiProcessStats,

    #[serde(default)]
    reserved: MiStatCount,

    #[serde(default)]
    committed: MiStatCount,

    #[serde(default)]
    mmap_calls: i64,

    #[serde(default)]
    commit_calls: i64,

    #[serde(default)]
    purge_calls: i64,

    #[serde(default)]
    purged: i64,
}

#[derive(Debug)]
struct MemoryDoctorStats {
    /*
     * OS process statistics.
     */
    process_rss: u64,
    process_peak_rss: u64,

    process_commit: u64,
    process_peak_commit: u64,

    /*
     * mimalloc VM statistics.
     *
     * committed 是 mimalloc 当前已经 commit 的 VM memory。
     *
     * 注意：
     * 不能把它叫 Redis allocator.resident。
     */
    allocator_committed: u64,
    allocator_peak_committed: u64,

    purge_calls: u64,
    purged_bytes: u64,
}

impl MemoryDoctorStats {
    fn collect() -> Result<Self, &'static str> {
        let json =
            MiMalloc::stats_json()
                .map_err(|_| {
                    "failed to get mimalloc stats"
                })?;

        let raw: MiMallocStats =
            serde_json::from_slice(json.to_bytes())
                .map_err(|_| {
                    "failed to parse mimalloc stats"
                })?;

        Ok(Self {
            process_rss:
            raw.process.rss_current,

            process_peak_rss:
            raw.process.rss_peak,

            process_commit:
            raw.process.commit_current,

            process_peak_commit:
            raw.process.commit_peak,

            allocator_committed:
            non_negative(
                raw.committed.current
            ),

            allocator_peak_committed:
            non_negative(
                raw.committed.peak
            ),

            purge_calls:
            non_negative(
                raw.purge_calls
            ),

            purged_bytes:
            non_negative(
                raw.purged
            ),
        })
    }

    /*
     * 当前能够可靠观察到的 process / allocator footprint。
     *
     * 这里不是 Redis used_memory。
     *
     * 只是用于判断：
     *
     *     当前实例是否小到 MEMORY DOCTOR
     *     没有足够样本做分析。
     *
     * 使用 max 是为了避免：
     *
     * - 某些系统 rss_current 不可用
     * - RSS 和 commit 本身语义不同
     *
     * 我们不会把这个值暴露成 Redis used_memory。
     */
    fn current_memory_footprint(&self) -> u64 {
        self.process_rss
            .max(self.process_commit)
            .max(self.allocator_committed)
    }

    fn diagnose(self) -> MemoryDoctorReport {
        let footprint =
            self.current_memory_footprint();

        /*
         * Redis MEMORY DOCTOR 对很小的实例
         * 不进行进一步判断。
         */
        if footprint < MIN_MEMORY_FOR_DIAGNOSIS {
            return MemoryDoctorReport::TooLittleMemory {
                footprint,
            };
        }

        let mut issues =
            Vec::with_capacity(4);

        /*
         * Redis 原始 MEMORY DOCTOR 有：
         *
         *     used_memory_peak / used_memory > 1.5
         *
         * mimalloc release build 没有可靠的
         * logical allocation peak。
         *
         * 因此这里用我们确实能获得的三个
         * 同类 peak/current pair 判断：
         *
         * - process RSS
         * - process commit
         * - mimalloc committed
         *
         * 它们不是 Redis used_memory，
         * 所以报告中明确标出具体 metric。
         */

        if high_peak(
            self.process_peak_rss,
            self.process_rss,
        ) {
            issues.push(
                MemoryIssue::HighProcessRssPeak {
                    current:
                    self.process_rss,

                    peak:
                    self.process_peak_rss,
                },
            );
        }

        if high_peak(
            self.process_peak_commit,
            self.process_commit,
        ) {
            issues.push(
                MemoryIssue::HighProcessCommitPeak {
                    current:
                    self.process_commit,

                    peak:
                    self.process_peak_commit,
                },
            );
        }

        if high_peak(
            self.allocator_peak_committed,
            self.allocator_committed,
        ) {
            issues.push(
                MemoryIssue::HighAllocatorCommitPeak {
                    current:
                    self.allocator_committed,

                    peak:
                    self.allocator_peak_committed,
                },
            );
        }

        /*
         * Redis 原版还能诊断：
         *
         * - fragmentation ratio
         * - allocator fragmentation
         * - allocator RSS
         * - client buffers
         * - replica buffers
         * - cached scripts
         *
         * 当前这些信息无法仅凭 mimalloc release
         * stats 严格获得，因此这里不伪造。
         */

        if issues.is_empty() {
            MemoryDoctorReport::Healthy {
                stats: self,
            }
        } else {
            MemoryDoctorReport::Issues {
                stats: self,
                issues,
            }
        }
    }
}

#[derive(Debug)]
enum MemoryIssue {
    HighProcessRssPeak {
        current: u64,
        peak: u64,
    },

    HighProcessCommitPeak {
        current: u64,
        peak: u64,
    },

    HighAllocatorCommitPeak {
        current: u64,
        peak: u64,
    },
}

#[derive(Debug)]
enum MemoryDoctorReport {
    TooLittleMemory {
        footprint: u64,
    },

    Healthy {
        stats: MemoryDoctorStats,
    },

    Issues {
        stats: MemoryDoctorStats,
        issues: Vec<MemoryIssue>,
    },
}

impl MemoryDoctorReport {
    fn into_value(self) -> Value {
        Value::VerbatimString {
            /*
             * Redis MEMORY DOCTOR 在 RESP3 下
             * 返回 txt verbatim string。
             *
             * RESP2 encoder 应自动降级成 BulkString。
             */
            format: "txt".to_string(),

            data: Bytes::from(
                self.render().into_bytes()
            ),
        }
    }

    fn render(self) -> String {
        match self {
            MemoryDoctorReport::TooLittleMemory {
                footprint,
            } => {
                format!(
                    concat!(
                    "This instance is using very little memory, ",
                    "so MEMORY DOCTOR does not have enough data ",
                    "to perform meaningful diagnostics.\n\n",
                    "Observed memory footprint: {}.\n",
                    ),
                    human_bytes(footprint),
                )
            }

            MemoryDoctorReport::Healthy {
                stats,
            } => {
                let mut report =
                    String::with_capacity(512);

                report.push_str(
                    "Hi Sam, I can't find any obvious memory "
                );

                report.push_str(
                    "issue in this instance.\n\n"
                );

                write_summary(
                    &mut report,
                    &stats,
                );

                report.push_str(
                    "\nThe available mimalloc and process statistics "
                );

                report.push_str(
                    "do not indicate a significant historical "
                );

                report.push_str(
                    "memory peak.\n"
                );

                report
            }

            MemoryDoctorReport::Issues {
                stats,
                issues,
            } => {
                let mut report =
                    String::with_capacity(2048);

                report.push_str(
                    "MEMORY DOCTOR detected possible memory "
                );

                report.push_str(
                    "issues in this instance:\n\n"
                );

                for issue in issues {
                    issue.write_report(
                        &mut report
                    );
                }

                report.push_str(
                    "Memory summary:\n"
                );

                write_summary(
                    &mut report,
                    &stats,
                );

                report.push_str(
                    "\nIf the memory peak was temporary, "
                );

                report.push_str(
                    "MEMORY PURGE may help mimalloc return "
                );

                report.push_str(
                    "unused committed pages to the operating "
                );

                report.push_str(
                    "system.\n"
                );

                report
            }
        }
    }
}

impl MemoryIssue {
    fn write_report(
        &self,
        report: &mut String,
    ) {
        match self {
            MemoryIssue::HighProcessRssPeak {
                current,
                peak,
            } => {
                report.push_str(
                    " * Peak process RSS: the process previously "
                );

                report.push_str(
                    "used substantially more resident memory than "
                );

                report.push_str(
                    "it uses now.\n"
                );

                report.push_str(
                    "   Current RSS: "
                );

                report.push_str(
                    &human_bytes(*current)
                );

                report.push('\n');

                report.push_str(
                    "   Peak RSS: "
                );

                report.push_str(
                    &human_bytes(*peak)
                );

                report.push('\n');

                report.push_str(
                    "   Peak/current ratio: "
                );

                report.push_str(
                    &format!(
                        "{:.2}",
                        ratio(*peak, *current)
                    )
                );

                report.push_str(
                    "\n\n"
                );
            }

            MemoryIssue::HighProcessCommitPeak {
                current,
                peak,
            } => {
                report.push_str(
                    " * Peak process commit: the process previously "
                );

                report.push_str(
                    "had substantially more committed memory than "
                );

                report.push_str(
                    "it has now.\n"
                );

                report.push_str(
                    "   Current commit: "
                );

                report.push_str(
                    &human_bytes(*current)
                );

                report.push('\n');

                report.push_str(
                    "   Peak commit: "
                );

                report.push_str(
                    &human_bytes(*peak)
                );

                report.push('\n');

                report.push_str(
                    "   Peak/current ratio: "
                );

                report.push_str(
                    &format!(
                        "{:.2}",
                        ratio(*peak, *current)
                    )
                );

                report.push_str(
                    "\n\n"
                );
            }

            MemoryIssue::HighAllocatorCommitPeak {
                current,
                peak,
            } => {
                report.push_str(
                    " * Peak allocator commit: mimalloc previously "
                );

                report.push_str(
                    "had substantially more committed memory than "
                );

                report.push_str(
                    "it has now.\n"
                );

                report.push_str(
                    "   Current committed: "
                );

                report.push_str(
                    &human_bytes(*current)
                );

                report.push('\n');

                report.push_str(
                    "   Peak committed: "
                );

                report.push_str(
                    &human_bytes(*peak)
                );

                report.push('\n');

                report.push_str(
                    "   Peak/current ratio: "
                );

                report.push_str(
                    &format!(
                        "{:.2}",
                        ratio(*peak, *current)
                    )
                );

                report.push_str(
                    "\n\n"
                );
            }
        }
    }
}

fn write_summary(
    report: &mut String,
    stats: &MemoryDoctorStats,
) {
    report.push_str(
        "Process RSS: "
    );

    report.push_str(
        &human_bytes(
            stats.process_rss
        )
    );

    report.push_str(
        "\nPeak process RSS: "
    );

    report.push_str(
        &human_bytes(
            stats.process_peak_rss
        )
    );

    report.push_str(
        "\nProcess commit: "
    );

    report.push_str(
        &human_bytes(
            stats.process_commit
        )
    );

    report.push_str(
        "\nPeak process commit: "
    );

    report.push_str(
        &human_bytes(
            stats.process_peak_commit
        )
    );

    report.push_str(
        "\nAllocator committed: "
    );

    report.push_str(
        &human_bytes(
            stats.allocator_committed
        )
    );

    report.push_str(
        "\nPeak allocator committed: "
    );

    report.push_str(
        &human_bytes(
            stats.allocator_peak_committed
        )
    );

    report.push_str(
        "\nAllocator purge calls: "
    );

    report.push_str(
        &stats.purge_calls.to_string()
    );

    report.push_str(
        "\nAllocator purged bytes: "
    );

    report.push_str(
        &human_bytes(
            stats.purged_bytes
        )
    );

    report.push('\n');
}

fn high_peak(
    peak: u64,
    current: u64,
) -> bool {
    /*
     * 0 通常表示当前平台 / mimalloc build
     * 没有提供这个 metric。
     *
     * 此时直接跳过，不能产生 false positive。
     */
    if current == 0
        || peak == 0
        || peak <= current
    {
        return false;
    }

    ratio(
        peak,
        current,
    ) > HIGH_PEAK_RATIO
}

fn ratio(
    numerator: u64,
    denominator: u64,
) -> f64 {
    if denominator == 0 {
        return 0.0;
    }

    numerator as f64
        / denominator as f64
}

fn non_negative(
    value: i64,
) -> u64 {
    u64::try_from(value)
        .unwrap_or(0)
}

fn human_bytes(
    bytes: u64,
) -> String {
    const KIB: u64 =
        1024;

    const MIB: u64 =
        1024 * KIB;

    const GIB: u64 =
        1024 * MIB;

    const TIB: u64 =
        1024 * GIB;

    if bytes >= TIB {
        format!(
            "{:.1} TiB",
            bytes as f64
                / TIB as f64
        )
    } else if bytes >= GIB {
        format!(
            "{:.1} GiB",
            bytes as f64
                / GIB as f64
        )
    } else if bytes >= MIB {
        format!(
            "{:.1} MiB",
            bytes as f64
                / MIB as f64
        )
    } else if bytes >= KIB {
        format!(
            "{:.1} KiB",
            bytes as f64
                / KIB as f64
        )
    } else {
        format!(
            "{bytes} B"
        )
    }
}

pub struct MemoryDoctorCommand;

impl MemoryDoctorCommand {
    fn parse(
        items: &[Value],
    ) -> Result<(), ProtocolError> {
        if items.len() != 2 {
            return Err(
                ProtocolError::WrongArgCount(
                    "MEMORY DOCTOR"
                )
            );
        }

        let memory = items[0]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument(
                    "command"
                )
            )?;

        if !memory
            .as_ref()
            .eq_ignore_ascii_case(
                b"MEMORY"
            )
        {
            return Err(
                ProtocolError::InvalidArgument(
                    "command"
                )
            );
        }

        let doctor = items[1]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument(
                    "subcommand"
                )
            )?;

        if !doctor
            .as_ref()
            .eq_ignore_ascii_case(
                b"DOCTOR"
            )
        {
            return Err(
                ProtocolError::InvalidArgument(
                    "subcommand"
                )
            );
        }

        Ok(())
    }
}

#[async_trait]
impl SubCommand for MemoryDoctorCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        Self::parse(items)?;

        let stats =
            MemoryDoctorStats::collect()
                .map_err(|_| {
                    ProtocolError::InvalidArgument(
                        "mimalloc stats"
                    )
                })?;

        Ok(
            stats
                .diagnose()
                .into_value()
        )
    }
}
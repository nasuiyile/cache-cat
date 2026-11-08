//! Stream JSON replies without constructing a second document tree.

use serde::{Deserialize, Serialize};
use serde_json::ser::Formatter;
use std::io::{self, Write};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JsonFormatting {
    pub indent: Option<String>,
    pub newline: Option<String>,
    pub space: Option<String>,
}

impl JsonFormatting {
    pub fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>, serde_json::Error> {
        let formatter = JsonFormatter {
            options: self,
            containers: Vec::new(),
        };
        let mut serializer = serde_json::Serializer::with_formatter(Vec::new(), formatter);
        value.serialize(&mut serializer)?;
        Ok(serializer.into_inner())
    }
}

struct JsonFormatter<'a> {
    options: &'a JsonFormatting,
    containers: Vec<bool>,
}

impl JsonFormatter<'_> {
    fn line<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        if let Some(newline) = &self.options.newline {
            writer.write_all(newline.as_bytes())?;
        }
        if let Some(indent) = &self.options.indent {
            for _ in 0..self.containers.len() {
                writer.write_all(indent.as_bytes())?;
            }
        }
        Ok(())
    }

    fn open<W: Write + ?Sized>(&mut self, writer: &mut W, marker: &[u8]) -> io::Result<()> {
        self.containers.push(false);
        writer.write_all(marker)
    }

    fn close<W: Write + ?Sized>(&mut self, writer: &mut W, marker: &[u8]) -> io::Result<()> {
        if self.containers.pop().unwrap_or(false) {
            self.line(writer)?;
        }
        writer.write_all(marker)
    }

    fn element<W: Write + ?Sized>(&mut self, writer: &mut W, first: bool) -> io::Result<()> {
        if !first {
            writer.write_all(b",")?;
        }
        if let Some(nonempty) = self.containers.last_mut() {
            *nonempty = true;
        }
        self.line(writer)
    }
}

impl Formatter for JsonFormatter<'_> {
    // RedisJSON pins serde_json's earlier ryu formatting. Keep JSON replies
    // stable without changing the serializer used by unrelated commands.
    fn write_f64<W: Write + ?Sized>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(ryu::Buffer::new().format_finite(value).as_bytes())
    }

    fn write_f32<W: Write + ?Sized>(&mut self, writer: &mut W, value: f32) -> io::Result<()> {
        writer.write_all(ryu::Buffer::new().format_finite(value).as_bytes())
    }

    fn begin_array<W: Write + ?Sized>(&mut self, writer: &mut W) -> io::Result<()> {
        self.open(writer, b"[")
    }
    fn end_array<W: Write + ?Sized>(&mut self, writer: &mut W) -> io::Result<()> {
        self.close(writer, b"]")
    }
    fn begin_object<W: Write + ?Sized>(&mut self, writer: &mut W) -> io::Result<()> {
        self.open(writer, b"{")
    }
    fn end_object<W: Write + ?Sized>(&mut self, writer: &mut W) -> io::Result<()> {
        self.close(writer, b"}")
    }
    fn begin_array_value<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.element(writer, first)
    }
    fn begin_object_key<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.element(writer, first)
    }
    fn begin_object_value<W: Write + ?Sized>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b":")?;
        if let Some(space) = &self.options.space {
            writer.write_all(space.as_bytes())?;
        }
        Ok(())
    }
}

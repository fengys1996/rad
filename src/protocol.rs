use std::{io, str};

use serde_json::Value;

const HEADER_DELIMITER: &[u8] = b"\r\n\r\n";

#[derive(Debug, Clone)]
pub struct LspPacket {
    pub body: Vec<u8>,
}

impl LspPacket {
    pub fn from_body(body: Vec<u8>) -> Self {
        Self { body }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.body.len() + 32);
        out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", self.body.len()).as_bytes());
        out.extend_from_slice(&self.body);
        out
    }

    pub fn parse_json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }
}

#[derive(Default, Debug)]
pub struct LspPacketDecoder {
    buf: Vec<u8>,
}

impl LspPacketDecoder {
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_packet(&mut self) -> io::Result<Option<LspPacket>> {
        let Some(header_end) = find_header_end(&self.buf) else {
            return Ok(None);
        };

        let content_len = parse_content_length(&self.buf[..header_end])?;
        let body_start = header_end + HEADER_DELIMITER.len();
        let total_len = body_start + content_len;

        if self.buf.len() < total_len {
            return Ok(None);
        }

        let body = self.buf[body_start..total_len].to_vec();
        self.buf.drain(..total_len);
        Ok(Some(LspPacket::from_body(body)))
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(HEADER_DELIMITER.len())
        .position(|window| window == HEADER_DELIMITER)
}

fn parse_content_length(headers: &[u8]) -> io::Result<usize> {
    let headers = str::from_utf8(headers)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    for line in headers.split("\r\n") {
        let (name, value) = match line.split_once(':') {
            Some(parts) => parts,
            None => continue,
        };

        if name.trim().eq_ignore_ascii_case("content-length") {
            let len = value.trim().parse::<usize>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Content-Length: {err}"),
                )
            })?;
            return Ok(len);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "missing Content-Length header",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_handles_split_headers_and_body() {
        let mut decoder = LspPacketDecoder::default();
        decoder.push(b"Content-Length: 18\r\n");
        assert!(decoder.next_packet().unwrap().is_none());

        decoder.push(b"\r\n{\"jsonrpc\":\"2.0\"}");
        let packet = decoder.next_packet().unwrap().expect("packet should exist");
        assert_eq!(packet.body, br#"{"jsonrpc":"2.0"}"#);
    }
}

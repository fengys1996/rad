use std::str;

use bytes::{Buf, BytesMut};
use serde_json::Value;
use snafu::ResultExt;
use tokio_util::codec::Decoder;
use tokio_util::codec::FramedRead;

use crate::error::Error;
use crate::error::MissingContentLengthSnafu;
use crate::error::Result;
use crate::error::{InvalidContentLengthSnafu, InvalidHeaderUtf8Snafu, InvalidJsonSnafu};

pub type ClientId = u32;
pub type LspFrameStream<R> = FramedRead<R, LspFrameDecoder>;

const HEADER_DELIMITER: &[u8] = b"\r\n\r\n";

#[derive(Debug, Clone)]
pub struct LspFrame {
    pub body: Value,
}

impl LspFrame {
    pub fn new(body: Value) -> Self {
        Self { body }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let body = serde_json::to_vec(&self.body).context(InvalidJsonSnafu)?;
        let mut out = Vec::with_capacity(body.len() + 32);
        out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn is_method(&self, target: &str) -> bool {
        self.as_json()
            .get("method")
            .and_then(Value::as_str)
            .map(|method| method == target)
            .unwrap_or(false)
    }

    pub fn is_request_method(&self, target: &str) -> bool {
        let json_val = self.as_json();
        json_val
            .get("method")
            .and_then(Value::as_str)
            .map(|method| method == target && json_val.get("id").is_some())
            .unwrap_or(false)
    }

    pub fn as_json(&self) -> &Value {
        &self.body
    }
}

#[derive(Default, Debug)]
pub struct LspFrameDecoder;

impl LspFrameDecoder {
    pub fn decode_packet(&mut self, src: &mut BytesMut) -> Result<Option<LspFrame>> {
        let Some(header_end) = find_header_end(src.as_ref()) else {
            return Ok(None);
        };

        let content_len = parse_content_length(&src[..header_end])?;
        let body_start = header_end + HEADER_DELIMITER.len();
        let total_len = body_start + content_len;

        if src.len() < total_len {
            return Ok(None);
        }

        let body_bytes = src[body_start..total_len].to_vec();
        src.advance(total_len);
        let body = serde_json::from_slice(&body_bytes).context(InvalidJsonSnafu)?;
        Ok(Some(LspFrame::new(body)))
    }
}

impl Decoder for LspFrameDecoder {
    type Item = LspFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        self.decode_packet(src)
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(HEADER_DELIMITER.len())
        .position(|window| window == HEADER_DELIMITER)
}

fn parse_content_length(headers: &[u8]) -> Result<usize> {
    let headers = str::from_utf8(headers).context(InvalidHeaderUtf8Snafu)?;

    for line in headers.split("\r\n") {
        let (name, value) = match line.split_once(':') {
            Some(parts) => parts,
            None => continue,
        };

        if name.trim().eq_ignore_ascii_case("content-length") {
            let len = value
                .trim()
                .parse::<usize>()
                .context(InvalidContentLengthSnafu)?;
            return Ok(len);
        }
    }

    MissingContentLengthSnafu.fail()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[test]
    fn decoder_handles_split_headers_and_body() {
        let body = br#"{"jsonrpc":"2.0"}"#;
        let expected: Value = serde_json::from_slice(body).unwrap();
        let mut decoder = LspFrameDecoder;
        let mut src = BytesMut::new();
        src.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
        assert!(decoder.decode_packet(&mut src).unwrap().is_none());

        src.extend_from_slice(b"\r\n");
        src.extend_from_slice(body);
        let packet = decoder
            .decode_packet(&mut src)
            .unwrap()
            .expect("packet should exist");
        assert_eq!(packet.body, expected);
    }

    #[tokio::test]
    async fn frame_stream_reads_split_frame() {
        let body = br#"{"jsonrpc":"2.0"}"#;
        let expected: Value = serde_json::from_slice(body).unwrap();
        let bytes = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        let mut payload = bytes;
        payload.extend_from_slice(body);
        let reader = tokio::io::duplex(64);
        let (mut tx, rx) = reader;
        let writer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;

            tx.write_all(&payload[..20]).await.unwrap();
            tx.write_all(&payload[20..]).await.unwrap();
        });

        let mut stream = LspFrameStream::new(rx, LspFrameDecoder);
        let frame = stream
            .next()
            .await
            .expect("frame should exist")
            .expect("frame should decode");
        assert_eq!(frame.body, expected);

        writer.await.unwrap();
    }
}

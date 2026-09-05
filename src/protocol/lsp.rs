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

#[derive(Default, Debug)]
pub struct LspFrameDecoder;

impl Decoder for LspFrameDecoder {
    type Item = LspFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        self.decode_packet(src)
    }
}

impl LspFrameDecoder {
    pub fn decode_packet(&mut self, src: &mut BytesMut) -> Result<Option<LspFrame>> {
        let Some((_header_end, body_start, total_len)) = decode_frame_bounds(src)? else {
            return Ok(None);
        };
        let body_bytes = src[body_start..total_len].to_vec();
        src.advance(total_len);
        let body = serde_json::from_slice(&body_bytes).context(InvalidJsonSnafu)?;
        Ok(Some(LspFrame::new(body)))
    }
}

fn decode_frame_bounds(src: &BytesMut) -> Result<Option<(usize, usize, usize)>> {
    let Some(header_end) = find_header_end(src.as_ref()) else {
        return Ok(None);
    };

    let content_len = parse_content_length(&src[..header_end])?;
    let body_start = header_end + HEADER_DELIMITER.len();
    let total_len = body_start + content_len;

    if src.len() < total_len {
        return Ok(None);
    }

    Ok(Some((header_end, body_start, total_len)))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LspFrame {
    body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum JsonRpcId {
    Number(i64),
    String(String),
}

impl LspFrame {
    pub(crate) fn new(body: Value) -> Self {
        Self { body }
    }

    pub(crate) fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let body = serde_json::from_slice(bytes).context(InvalidJsonSnafu)?;
        Ok(Self::new(body))
    }

    pub(crate) fn to_json_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.body).context(InvalidJsonSnafu)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let body = self.to_json_bytes()?;
        let mut out = Vec::with_capacity(body.len() + 32);
        out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn is_method(&self, target: &str) -> bool {
        self.method() == Some(target)
    }

    pub(crate) fn method(&self) -> Option<&str> {
        self.body.get("method").and_then(Value::as_str)
    }

    pub fn is_request_method(&self, target: &str) -> bool {
        self.is_method(target) && self.body.get("id").is_some()
    }

    pub(crate) fn is_request(&self) -> bool {
        self.body.get("method").is_some() && self.body.get("id").is_some()
    }

    pub(crate) fn is_response(&self) -> bool {
        self.body.get("id").is_some()
            && (self.body.get("result").is_some() || self.body.get("error").is_some())
    }

    pub(crate) fn is_success_response(&self) -> bool {
        self.body.get("id").is_some() && self.body.get("result").is_some()
    }

    pub(crate) fn id(&self) -> Option<JsonRpcId> {
        JsonRpcId::from_value(self.body.get("id")?)
    }

    pub(crate) fn set_id(&mut self, id: JsonRpcId) {
        if let Some(object) = self.body.as_object_mut() {
            object.insert("id".to_string(), id.into_value());
        }
    }

    pub(crate) fn cancel_request_id(&self) -> Option<JsonRpcId> {
        if !self.is_method("$/cancelRequest") {
            return None;
        }

        JsonRpcId::from_value(self.body.get("params")?.get("id")?)
    }

    pub(crate) fn set_cancel_request_id(&mut self, id: JsonRpcId) {
        if let Some(params) = self.body.get_mut("params").and_then(Value::as_object_mut) {
            params.insert("id".to_string(), id.into_value());
        }
    }

    pub(crate) fn workspace_key(&self) -> Option<String> {
        if !self.is_method("initialize") {
            return None;
        }

        let params = self.body.get("params")?;
        let workspace_uri = params
            .get("workspaceFolders")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("uri"))
            .and_then(Value::as_str);
        if let Some(uri) = workspace_uri {
            return Some(uri.to_string());
        }

        for field in ["rootUri", "rootPath"] {
            if let Some(value) = params.get(field).and_then(Value::as_str)
                && !value.is_empty()
            {
                return Some(value.to_string());
            }
        }

        None
    }

    pub(crate) fn shutdown_response(&self) -> Option<Self> {
        if !self.is_request_method("shutdown") {
            return None;
        }

        let id = self.id()?.into_value();
        Some(Self::new(serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": null,
        })))
    }

    #[cfg(test)]
    pub(crate) fn as_json(&self) -> &Value {
        &self.body
    }
}

impl JsonRpcId {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Number(number) => number.as_i64().map(Self::Number),
            Value::String(string) => Some(Self::String(string.clone())),
            _ => None,
        }
    }

    fn into_value(self) -> Value {
        match self {
            Self::Number(number) => Value::Number(number.into()),
            Self::String(string) => Value::String(string),
        }
    }
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
        assert_eq!(packet.as_json(), &expected);
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
        assert_eq!(frame.as_json(), &expected);

        writer.await.unwrap();
    }
}

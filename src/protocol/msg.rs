use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio_util::codec::{Decoder, Encoder, FramedRead};

use crate::error::{Error, InvalidJsonSnafu, PlainTextSnafu, Result};

use super::lsp::LspFrame;

pub type RadFrameStream<R> = FramedRead<R, RadFrameCocdec>;

const HEADER_LEN: usize = 5;
const KIND_LSP: u8 = 1;
const KIND_CONTROL: u8 = 2;
const MAX_BODY_LEN: usize = 64 * 1024 * 1024;

#[derive(Default, Debug)]
pub struct RadFrameCocdec;

impl Encoder<RadMessage> for RadFrameCocdec {
    type Error = Error;

    fn encode(&mut self, item: RadMessage, dst: &mut BytesMut) -> Result<()> {
        let (kind, body) = match item {
            RadMessage::Lsp(frame) => {
                let body = serde_json::to_vec(&frame.body).context(InvalidJsonSnafu)?;
                (KIND_LSP, body)
            }
            RadMessage::Control(message) => {
                let body = serde_json::to_vec(&message).context(InvalidJsonSnafu)?;
                (KIND_CONTROL, body)
            }
        };

        if body.len() > MAX_BODY_LEN {
            return PlainTextSnafu {
                msg: format!("rad message body too large: {}", body.len()),
            }
            .fail();
        }

        dst.reserve(HEADER_LEN + body.len());
        dst.extend_from_slice(&[kind]);
        dst.extend_from_slice(&(body.len() as u32).to_be_bytes());
        dst.extend_from_slice(&body);
        Ok(())
    }
}

impl Decoder for RadFrameCocdec {
    type Item = RadMessage;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        self.decode_packet(src)
    }
}

impl RadFrameCocdec {
    pub fn decode_packet(&mut self, src: &mut BytesMut) -> Result<Option<RadMessage>> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let kind = src[0];
        let body_len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
        if body_len > MAX_BODY_LEN {
            return PlainTextSnafu {
                msg: format!("rad message body too large: {body_len}"),
            }
            .fail();
        }

        let total_len = HEADER_LEN + body_len;
        if src.len() < total_len {
            return Ok(None);
        }

        src.advance(HEADER_LEN);
        let body = src.split_to(body_len).to_vec();

        match kind {
            KIND_LSP => {
                let body = serde_json::from_slice(&body).context(InvalidJsonSnafu)?;
                Ok(Some(RadMessage::Lsp(LspFrame::new(body))))
            }
            KIND_CONTROL => {
                let message = serde_json::from_slice(&body).context(InvalidJsonSnafu)?;
                Ok(Some(RadMessage::Control(message)))
            }
            _ => PlainTextSnafu {
                msg: format!("unknown rad message kind: {kind}"),
            }
            .fail(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RadMessage {
    Lsp(LspFrame),
    Control(ControlMessage),
}

impl RadMessage {
    pub fn lsp(frame: LspFrame) -> Self {
        Self::Lsp(frame)
    }

    pub fn control(message: ControlMessage) -> Self {
        Self::Control(message)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = BytesMut::new();
        RadFrameCocdec.encode(self.clone(), &mut bytes)?;
        Ok(bytes.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    StatusRequest,
    StatusResponse { status: ServerStatus },
    ClearRequest { force: bool },
    ClearResponse { cleared: Vec<ClearedInstance> },
    Error { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClearedInstance {
    pub workspace: String,
    pub pid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerStatus {
    pub instances: Vec<InstanceStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceStatus {
    pub workspace: String,
    pub pid: u32,
    pub client_count: usize,
    pub idle_secs: i64,
    pub healthy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_lsp_message() {
        let message = RadMessage::lsp(LspFrame::new(serde_json::json!({
            "jsonrpc": "2.0",
            "method": "test"
        })));
        let bytes = message.to_bytes().unwrap();
        let decoded = RadFrameCocdec
            .decode_packet(&mut BytesMut::from(bytes.as_slice()))
            .unwrap()
            .expect("message should decode");

        assert_eq!(message, decoded);
    }

    #[test]
    fn round_trips_control_message() {
        let message = RadMessage::control(ControlMessage::StatusRequest);
        let bytes = message.to_bytes().unwrap();
        let decoded = RadFrameCocdec
            .decode_packet(&mut BytesMut::from(bytes.as_slice()))
            .unwrap()
            .expect("message should decode");

        assert_eq!(message, decoded);
    }
}

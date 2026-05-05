use std::io;

use tokio::net::TcpStream;

pub mod control;
pub mod lsp;

pub use control::{
    ControlFrame, MAGIC, TYPE_STATUS_REQ, TYPE_STATUS_RESP, decode_frame, encode_frame,
    encode_status_req, encode_status_resp,
};
pub use lsp::{ClientId, LspPacket, LspPacketDecoder, LspPacketStream};

pub enum RadMessage {
    Lsp(LspPacket),
    Control(ControlFrame),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RadMessageKind {
    Lsp,
    Control,
}

impl RadMessage {
    pub async fn peek_kind(stream: &TcpStream) -> io::Result<RadMessageKind> {
        let mut buf = [0u8; 4];
        let n = stream.peek(&mut buf).await?;
        if n < 4 {
            return Ok(RadMessageKind::Lsp);
        }
        if u32::from_be_bytes(buf) == MAGIC {
            Ok(RadMessageKind::Control)
        } else {
            Ok(RadMessageKind::Lsp)
        }
    }
}

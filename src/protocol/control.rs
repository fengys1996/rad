use std::io;

use serde_json::Value;

pub const MAGIC: u32 = 0x1212_3001;
pub const VERSION: u8 = 1;

pub const TYPE_STATUS_REQ: u16 = 0x0001;
pub const TYPE_STATUS_RESP: u16 = 0x0002;

const HEADER_LEN: usize = 13;

#[derive(Debug, Clone)]
pub struct ControlFrame {
    pub msg_type: u16,
    pub payload: Vec<u8>,
}

pub fn encode_frame(msg_type: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.push(VERSION);
    out.extend_from_slice(&msg_type.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn decode_frame(bytes: &[u8]) -> io::Result<ControlFrame> {
    if bytes.len() < HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "control frame header too short",
        ));
    }

    let magic = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid control frame magic",
        ));
    }
    if bytes[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported control frame version",
        ));
    }

    let msg_type = u16::from_be_bytes([bytes[5], bytes[6]]);
    let payload_len = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
    if bytes.len() != HEADER_LEN + payload_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control frame payload length mismatch",
        ));
    }

    Ok(ControlFrame {
        msg_type,
        payload: bytes[HEADER_LEN..].to_vec(),
    })
}

pub fn encode_status_req() -> Vec<u8> {
    encode_frame(TYPE_STATUS_REQ, b"")
}

pub fn encode_status_resp(payload: &Value) -> io::Result<Vec<u8>> {
    let payload = serde_json::to_vec(payload)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    Ok(encode_frame(TYPE_STATUS_RESP, &payload))
}

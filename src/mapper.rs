use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use serde_json::{Map, Number, Value};
use tracing::debug;

use crate::protocol::{ClientId, LspPacket};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum JsonRpcId {
    Number(i64),
    String(String),
}

impl JsonRpcId {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Number(num) => num.as_i64().map(Self::Number),
            Value::String(s) => Some(Self::String(s.clone())),
            _ => None,
        }
    }

    fn to_value(&self) -> Value {
        match self {
            Self::Number(n) => Value::Number(Number::from(*n)),
            Self::String(s) => Value::String(s.clone()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ReqIdMapper {
    next_global_id: Arc<AtomicU64>,
    global_to_client: Arc<DashMap<JsonRpcId, PendingReq>>,
    client_to_global: Arc<DashMap<(ClientId, JsonRpcId), JsonRpcId>>,
    init_resp_cache: Arc<RwLock<Option<Value>>>,
}

#[derive(Clone)]
struct PendingReq {
    client_id: ClientId,
    raw_req_id: JsonRpcId,
    method: String,
}

impl ReqIdMapper {
    pub(crate) fn new() -> Self {
        Self {
            next_global_id: Arc::new(AtomicU64::new(1)),
            global_to_client: Arc::new(DashMap::new()),
            client_to_global: Arc::new(DashMap::new()),
            init_resp_cache: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) fn rewrite_client_packet(
        &self,
        cid: ClientId,
        mut packet: LspPacket,
        pid: u32,
    ) -> LspPacket {
        let Some(obj) = packet.body.as_object_mut() else {
            return packet;
        };

        self.remap_req_id(cid, obj, pid);
        self.remap_cancel_req(cid, obj, pid);

        packet
    }

    fn remap_req_id(&self, cid: u32, obj: &mut Map<String, Value>, pid: u32) {
        if !is_req(obj) {
            return;
        }

        let Some(raw_req_id) = obj.get("id").and_then(JsonRpcId::from_value) else {
            return;
        };

        let global_raw = self.next_global_id.fetch_add(1, Ordering::Relaxed) as i64;
        let global_id = JsonRpcId::Number(global_raw);

        let method = obj
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let req = PendingReq {
            client_id: cid,
            raw_req_id: raw_req_id.clone(),
            method,
        };

        self.global_to_client.insert(global_id.clone(), req);
        self.client_to_global
            .insert((cid, raw_req_id.clone()), global_id.clone());

        obj.insert("id".to_string(), global_id.to_value());

        debug!(
            pid,
            cid,
            local_id = ?raw_req_id,
            global_id = global_raw,
            "remapped client request id"
        );
    }

    fn remap_cancel_req(&self, client_id: u32, obj: &mut Map<String, Value>, pid: u32) {
        let Some(Value::String(method)) = obj.get("method") else {
            return;
        };

        if method != "$/cancelRequest" {
            return;
        }

        let Some(cancel_id) = obj
            .get("params")
            .and_then(Value::as_object)
            .and_then(|params| params.get("id"))
            .and_then(JsonRpcId::from_value)
        else {
            return;
        };

        let Some(global_id) = self
            .client_to_global
            .get(&(client_id, cancel_id.clone()))
            .map(|entry| entry.value().clone())
        else {
            debug!(
                pid,
                client_id,
                cancel_id = ?cancel_id,
                "cancel request id not found in mapping"
            );
            return;
        };

        if let Some(params) = obj.get_mut("params").and_then(Value::as_object_mut) {
            params.insert("id".to_string(), global_id.to_value());
        }

        debug!(
            pid,
            client_id,
            cancel_id = ?cancel_id,
            mapped_id = ?global_id,
            "rewrote cancel request id"
        );
    }

    pub(crate) fn rewrite_ra_packet(
        &self,
        packet: LspPacket,
        active_client_id: u32,
        pid: u32,
    ) -> RoutedPacket {
        let mut json = packet.body.clone();

        let Some(obj) = json.as_object_mut() else {
            return RoutedPacket {
                client_id: active_client_id,
                bytes: packet.to_bytes(),
            };
        };

        if is_resp(obj)
            && let Some(global_id) = obj.get("id").and_then(JsonRpcId::from_value)
            && let Some((_, pending)) = self.global_to_client.remove(&global_id)
        {
            if pending.method == "initialize"
                && let Some(result) = obj.get("result").cloned()
                && let Ok(mut slot) = self.init_resp_cache.write()
            {
                *slot = Some(result);
            }

            self.client_to_global
                .remove(&(pending.client_id, pending.raw_req_id.clone()));
            obj.insert("id".to_string(), pending.raw_req_id.to_value());

            let bytes = LspPacket::from_body(json).to_bytes();

            debug!(
                pid,
                client_id = pending.client_id,
                global_id = ?global_id,
                local_id = ?pending.raw_req_id,
                "restored response id for client"
            );

            return RoutedPacket {
                client_id: pending.client_id,
                bytes,
            };
        }

        RoutedPacket {
            client_id: active_client_id,
            bytes: packet.to_bytes(),
        }
    }

    pub(crate) fn initialize_response_from_cache(&self, request_id: Value) -> Option<Vec<u8>> {
        let result = self.init_resp_cache.read().ok()?.clone()?;
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result,
        });
        let body = serde_json::to_vec(&response).ok()?;
        let json: Value = serde_json::from_slice(&body).ok()?;
        Some(LspPacket::from_body(json).to_bytes())
    }
}

pub(crate) struct RoutedPacket {
    pub(crate) client_id: u32,
    pub(crate) bytes: Vec<u8>,
}

fn is_req(obj: &Map<String, Value>) -> bool {
    obj.contains_key("method") && obj.contains_key("id")
}

fn is_resp(obj: &Map<String, Value>) -> bool {
    obj.contains_key("id") && (obj.contains_key("result") || obj.contains_key("error"))
}

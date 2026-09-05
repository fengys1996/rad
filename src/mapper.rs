use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use tracing::debug;

use crate::error::Result;
use crate::protocol::lsp::JsonRpcId;
use crate::protocol::{ClientId, LspFrame};

#[derive(Clone)]
pub(crate) struct ReqIdMapper {
    next_global_id: Arc<AtomicU64>,
    global_to_client: Arc<DashMap<JsonRpcId, PendingReq>>,
    client_to_global: Arc<DashMap<(ClientId, JsonRpcId), JsonRpcId>>,
    init_resp_cache: Arc<RwLock<Option<LspFrame>>>,
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
        mut packet: LspFrame,
        pid: u32,
    ) -> LspFrame {
        self.remap_req_id(cid, &mut packet, pid);
        self.remap_cancel_req(cid, &mut packet, pid);

        packet
    }

    fn remap_req_id(&self, cid: u32, packet: &mut LspFrame, pid: u32) {
        if !packet.is_request() {
            return;
        }

        let Some(raw_req_id) = packet.id() else {
            return;
        };

        let global_raw = self.next_global_id.fetch_add(1, Ordering::Relaxed) as i64;
        let global_id = JsonRpcId::Number(global_raw);

        let method = packet.method().unwrap_or_default().to_string();
        let req = PendingReq {
            client_id: cid,
            raw_req_id: raw_req_id.clone(),
            method,
        };

        self.global_to_client.insert(global_id.clone(), req);
        self.client_to_global
            .insert((cid, raw_req_id.clone()), global_id.clone());

        packet.set_id(global_id);

        debug!(
            pid,
            cid,
            local_id = ?raw_req_id,
            global_id = global_raw,
            "remapped client request id"
        );
    }

    fn remap_cancel_req(&self, client_id: u32, packet: &mut LspFrame, pid: u32) {
        let Some(cancel_id) = packet.cancel_request_id() else {
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

        packet.set_cancel_request_id(global_id.clone());

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
        packet: LspFrame,
        active_client_id: u32,
        pid: u32,
    ) -> Result<RoutedPacket> {
        if packet.is_response()
            && let Some(global_id) = packet.id()
            && let Some((_, pending)) = self.global_to_client.remove(&global_id)
        {
            if pending.method == "initialize"
                && packet.is_success_response()
                && let Ok(mut slot) = self.init_resp_cache.write()
            {
                *slot = Some(packet.clone());
            }

            self.client_to_global
                .remove(&(pending.client_id, pending.raw_req_id.clone()));
            let mut frame = packet;
            frame.set_id(pending.raw_req_id.clone());

            debug!(
                pid,
                client_id = pending.client_id,
                global_id = ?global_id,
                local_id = ?pending.raw_req_id,
                "restored response id for client"
            );

            return Ok(RoutedPacket {
                client_id: pending.client_id,
                frame,
            });
        }

        Ok(RoutedPacket {
            client_id: active_client_id,
            frame: packet,
        })
    }

    pub(crate) fn initialize_response_from_cache(&self, id: JsonRpcId) -> Option<LspFrame> {
        let mut response = self.init_resp_cache.read().ok()?.clone()?;
        response.set_id(id);
        Some(response)
    }
}

pub(crate) struct RoutedPacket {
    pub(crate) client_id: u32,
    pub(crate) frame: LspFrame,
}

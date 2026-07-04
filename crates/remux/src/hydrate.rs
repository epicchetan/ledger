use crate::error::RpcError;
use crate::rpc::{send_notification, OutboundSender};
use serde::Serialize;
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use store::{RemoteStore, Store, StoreObjectId};

pub const HYDRATED_NOTIFICATION: &str = "remux/ledger/store/hydrated";

#[derive(Clone, Default)]
pub struct HydrateJobs {
    inner: Arc<Mutex<HashSet<StoreObjectId>>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HydrateStartDto {
    pub started: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub already_local: Option<bool>,
}

impl HydrateStartDto {
    fn started() -> Self {
        Self {
            started: true,
            already_local: None,
        }
    }

    fn already_local() -> Self {
        Self {
            started: false,
            already_local: Some(true),
        }
    }
}

impl HydrateJobs {
    pub fn start<S>(
        &self,
        store: Store<S>,
        id: StoreObjectId,
        output_tx: OutboundSender,
    ) -> Result<HydrateStartDto, RpcError>
    where
        S: RemoteStore + 'static,
    {
        if store.touch_valid_local_copy(&id)? {
            return Ok(HydrateStartDto::already_local());
        }

        if !self.try_insert(id.clone())? {
            return Err(RpcError::domain("hydrate in flight").with_data(json!({
                "id": id.to_string(),
            })));
        }

        let jobs = self.clone();
        tokio::spawn(async move {
            let result = store.hydrate(&id).await;
            jobs.remove(&id);

            let params = match result {
                Ok(_) => json!({
                    "id": id.to_string(),
                    "ok": true,
                }),
                Err(error) => json!({
                    "id": id.to_string(),
                    "ok": false,
                    "error": error.to_string(),
                }),
            };

            if let Err(error) = send_notification(&output_tx, HYDRATED_NOTIFICATION, params).await {
                eprintln!("[ledger-remux] failed to broadcast hydrate completion: {error}");
            }
        });

        Ok(HydrateStartDto::started())
    }

    fn try_insert(&self, id: StoreObjectId) -> Result<bool, RpcError> {
        let mut jobs = self
            .inner
            .lock()
            .map_err(|_| RpcError::domain("hydrate job set poisoned"))?;
        Ok(jobs.insert(id))
    }

    fn remove(&self, id: &StoreObjectId) {
        if let Ok(mut jobs) = self.inner.lock() {
            jobs.remove(id);
        }
    }
}

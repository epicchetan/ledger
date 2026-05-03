use anyhow::Result;
use ledger_domain::ProjectionFrame;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

pub fn projection_frame_digest(frames: &[ProjectionFrame]) -> Result<String> {
    let mut hasher = Sha256::new();

    for frame in frames {
        let value = canonicalize_json(&projection_digest_value(frame)?);
        let bytes = serde_json::to_vec(&value)?;
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }

    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

fn projection_digest_value(frame: &ProjectionFrame) -> Result<Value> {
    Ok(json!({
        "projection_key": frame.stamp.projection_key.to_string(),
        "generation": frame.stamp.generation,
        "feed_seq": frame.stamp.feed_seq,
        "feed_ts_ns": frame.stamp.feed_ts_ns,
        "source_first_ts_ns": frame.stamp.source_first_ts_ns,
        "source_last_ts_ns": frame.stamp.source_last_ts_ns,
        "batch_idx": frame.stamp.batch_idx,
        "cursor_ts_ns": frame.stamp.cursor_ts_ns,
        "op": frame.op,
        "output_schema": frame.stamp.output_schema.name.to_string(),
        "payload": frame.payload,
    }))
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut out = Map::new();
            for key in keys {
                out.insert(key.clone(), canonicalize_json(&map[key]));
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_domain::{
        ProjectionFrameOp, ProjectionFrameStamp, ProjectionOutputSchema, ProjectionSpec,
        SourceView, TemporalPolicy,
    };

    fn frame(payload: Value, produced_at_ns: &str, sequence: u64) -> ProjectionFrame {
        ProjectionFrame {
            stamp: ProjectionFrameStamp {
                session_id: "session-a".to_string(),
                replay_dataset_id: "dataset-a".to_string(),
                generation: 1,
                projection_key: ProjectionSpec::new("bbo", 1, json!({}))
                    .unwrap()
                    .key()
                    .unwrap(),
                output_schema: ProjectionOutputSchema::new("bbo_v1").unwrap(),
                feed_seq: 7,
                feed_ts_ns: "700".to_string(),
                source_first_ts_ns: Some("700".to_string()),
                source_last_ts_ns: Some("700".to_string()),
                batch_idx: 7,
                cursor_ts_ns: "700".to_string(),
                source_view: Some(SourceView::ExchangeTruth),
                temporal_policy: TemporalPolicy::Causal,
                produced_at_ns: produced_at_ns.to_string(),
                sequence,
            },
            op: ProjectionFrameOp::Replace,
            payload,
        }
    }

    #[test]
    fn projection_digest_ignores_produced_at() {
        let left =
            projection_frame_digest(&[frame(json!({ "bid_price": 100 }), "1000", 1)]).unwrap();
        let right =
            projection_frame_digest(&[frame(json!({ "bid_price": 100 }), "2000", 1)]).unwrap();

        assert_eq!(left, right);
    }

    #[test]
    fn projection_digest_ignores_frame_sequence() {
        let left =
            projection_frame_digest(&[frame(json!({ "bid_price": 100 }), "1000", 1)]).unwrap();
        let right =
            projection_frame_digest(&[frame(json!({ "bid_price": 100 }), "1000", 2)]).unwrap();

        assert_eq!(left, right);
    }

    #[test]
    fn projection_digest_changes_when_payload_changes() {
        let left =
            projection_frame_digest(&[frame(json!({ "bid_price": 100 }), "1000", 1)]).unwrap();
        let right =
            projection_frame_digest(&[frame(json!({ "bid_price": 101 }), "1000", 1)]).unwrap();

        assert_ne!(left, right);
    }
}

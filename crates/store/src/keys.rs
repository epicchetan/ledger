use ledger_domain::MarketDay;

#[derive(Clone, Debug)]
pub struct ObjectKeyBuilder {
    prefix: String,
}

impl Default for ObjectKeyBuilder {
    fn default() -> Self {
        Self::new("ledger/v1")
    }
}

impl ObjectKeyBuilder {
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into().trim_matches('/').to_string();
        Self { prefix }
    }

    pub fn raw_dbn_logical_key(&self, md: &MarketDay, dataset: &str, schema: &str) -> String {
        format!(
            "raw:databento:{dataset}:{schema}:{}:{}:{}",
            md.root, md.contract_symbol, md.market_date
        )
    }

    pub fn raw_dbn_key(&self, md: &MarketDay, dataset: &str, schema: &str, sha256: &str) -> String {
        format!(
            "{}/raw/databento/{}/{}/{}/{}/{}/raw.sha256={}.dbn.zst",
            self.prefix, dataset, schema, md.root, md.contract_symbol, md.market_date, sha256
        )
    }

    pub fn artifact_logical_key(&self, md: &MarketDay, kind: &str, schema_version: i64) -> String {
        format!(
            "artifact:{kind}:v{schema_version}:{}:{}:{}",
            md.root, md.contract_symbol, md.market_date
        )
    }

    pub fn artifact_key(
        &self,
        md: &MarketDay,
        kind: &str,
        schema_version: i64,
        input_sha256: &str,
        producer_version: &str,
        file_name: &str,
    ) -> String {
        format!(
            "{}/artifacts/{}/{}/{}/{}/schema=v{}/input={}/producer={}/{}",
            self.prefix,
            md.root,
            md.contract_symbol,
            md.market_date,
            kind,
            schema_version,
            input_sha256,
            sanitize_path_component(producer_version),
            file_name
        )
    }
}

pub fn sanitize_path_component(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn keys_are_stable() {
        let md =
            MarketDay::resolve_es("ESH6", NaiveDate::from_ymd_opt(2026, 3, 12).unwrap()).unwrap();
        let keys = ObjectKeyBuilder::default();
        assert_eq!(
            keys.raw_dbn_key(&md, "GLBX.MDP3", "mbo", "abc"),
            "ledger/v1/raw/databento/GLBX.MDP3/mbo/ES/ESH6/2026-03-12/raw.sha256=abc.dbn.zst"
        );
        assert_eq!(
            keys.artifact_key(&md, "event_store", 1, "raw", "dev", "events.v1.bin"),
            "ledger/v1/artifacts/ES/ESH6/2026-03-12/event_store/schema=v1/input=raw/producer=dev/events.v1.bin"
        );
    }
}

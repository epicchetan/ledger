use ledger::Ledger;
use ledger_store::R2ObjectStore;

pub type ApiLedger = Ledger<R2ObjectStore>;

#[derive(Clone)]
pub struct ApiState {
    pub ledger: ApiLedger,
}

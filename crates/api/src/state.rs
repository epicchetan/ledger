use store::R2Store;

#[derive(Clone)]
pub struct ApiState {
    pub store: R2Store,
}

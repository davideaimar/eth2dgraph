use serde::Serializer;

pub mod abi;
pub mod block;
pub mod contract_deployment;
pub mod contract_destruction;
pub mod error;
pub mod event;
pub mod function;
pub mod log;
pub mod skeleton;
pub mod trace;
pub mod transaction;
pub mod transfer;

pub trait SerializeDgraph {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer;
}

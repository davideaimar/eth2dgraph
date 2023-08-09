use std::str::FromStr;

use super::{abi::ABIToken, SerializeDgraph};
use ethers::utils::keccak256;
use primitive_types::H256;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorABI {
    pub name: String,
    pub inputs: Vec<ABIToken>,
}

impl ErrorABI {
    pub fn get_signature_hash(&self) -> H256 {
        if self.name.starts_with("Error_") {
            let sig = self.name.split('_').last().unwrap();
            if sig.len() == 64 {
                return H256::from_str(sig).unwrap();
            }
        }
        let param_types = self
            .inputs
            .iter()
            .map(|i| i.internal_type.clone())
            .collect::<Vec<String>>()
            .join(",");
        let sig = format!("{}({})", self.name, param_types);
        H256(keccak256(sig.as_bytes()))
    }

    pub fn get_input_types(&self) -> String {
        self.inputs
            .iter()
            .map(|i| i.internal_type.clone())
            .collect::<Vec<String>>()
            .join(",")
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ErrorABI", 5)?;
        let param_types = self
            .inputs
            .iter()
            .map(|i| i.internal_type.clone())
            .collect::<Vec<String>>()
            .join(",");
        let sig_hash = self.get_signature_hash();
        state.serialize_field("dgraph.type", "Error")?;
        state.serialize_field("uid", &format!("_:{:?}", sig_hash))?;
        state.serialize_field("Error.signature", &sig_hash)?;
        state.serialize_field("Error.name", &self.name)?;
        state.serialize_field("Error.inputs", &param_types)?;
        state.end()
    }
}

impl SerializeDgraph for ErrorABI {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

use std::str::FromStr;

use super::{abi::ABIToken, SerializeDgraph};
use ethers::utils::keccak256;
use primitive_types::H256;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventABI {
    pub name: String,
    pub inputs: Vec<ABIToken>,
}

impl EventABI {
    pub fn get_signature_hash(&self) -> H256 {
        if self.name.starts_with("Event_") {
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
        let mut state = serializer.serialize_struct("EventABI", 5)?;
        let param_types = self
            .inputs
            .iter()
            .map(|i| i.internal_type.clone())
            .collect::<Vec<String>>()
            .join(",");
        let sig_hash = self.get_signature_hash();
        state.serialize_field("dgraph.type", "Event")?;
        state.serialize_field("uid", &format!("_:{:?}", sig_hash))?;
        state.serialize_field("Event.signature", &format!("{:?}", sig_hash))?;
        state.serialize_field("Event.name", &self.name)?;
        state.serialize_field("Event.inputs", &param_types)?;
        state.end()
    }
}

impl SerializeDgraph for EventABI {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unresolved_signature() {
        let abi = EventABI {
            name: "Event_c0d559150c15862e872a031a8e11f466df4b16d14e736187f2e7fb162060f9d0"
                .to_string(),
            inputs: vec![],
        };
        assert_eq!(
            format!("{:?}", abi.get_signature_hash()),
            "0xc0d559150c15862e872a031a8e11f466df4b16d14e736187f2e7fb162060f9d0"
        );
    }
}

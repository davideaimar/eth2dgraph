use super::{abi::ABIToken, SerializeDgraph};
use ethers::utils::keccak256;
use primitive_types::H256;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FunctionABI {
    pub name: String,
    pub inputs: Vec<ABIToken>,
    pub outputs: Vec<ABIToken>,
    #[serde(rename = "stateMutability")]
    pub _state_mutability: String,
    #[serde(rename = "constant")]
    pub _constant: bool,
}

impl FunctionABI {
    pub fn get_signature_hash(&self) -> H256 {
        // Returned signature is not correct if the function is not resolved by the decompiler
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

    pub fn get_output_types(&self) -> String {
        self.outputs
            .iter()
            .map(|i| i.internal_type.clone())
            .collect::<Vec<String>>()
            .join(",")
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionABI", 7)?;
        let param_types = self
            .inputs
            .iter()
            .map(|i| i.internal_type.clone())
            .collect::<Vec<String>>()
            .join(",");
        let sig_hash = self.get_signature_hash();
        let sig_hash = format!("{:?}", sig_hash).to_string();
        let bytes_4 = if self.name.starts_with("Unresolved_") {
            self.name.split("_").collect::<Vec<&str>>()[1]
        } else {
            sig_hash.get(2..10).unwrap()
        };
        state.serialize_field("dgraph.type", "Function")?;
        state.serialize_field("uid", &format!("_:{}", sig_hash))?;
        state.serialize_field("Function.signature", &sig_hash)?;
        state.serialize_field("Function.bytes4", &bytes_4)?;
        state.serialize_field("Function.name", &self.name)?;
        state.serialize_field("Function.inputs", &param_types)?;
        state.serialize_field(
            "Function.outputs",
            &self
                .outputs
                .iter()
                .map(|i| i.internal_type.clone())
                .collect::<Vec<String>>()
                .join(","),
        )?;
        state.end()
    }
}

impl SerializeDgraph for FunctionABI {
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
    fn test_signature() {
        let abi = FunctionABI {
            name: "transfer".to_string(),
            inputs: vec![
                ABIToken {
                    _name: "to".to_string(),
                    internal_type: "address".to_string(),
                },
                ABIToken {
                    _name: "value".to_string(),
                    internal_type: "uint256".to_string(),
                },
            ],
            outputs: vec![],
            _state_mutability: "nonpayable".to_string(),
            _constant: false,
        };
        assert_eq!(
            format!("{:?}", abi.get_signature_hash()),
            "0xa9059cbb2ab09eb219583f4a59a5d0623ade346d962bcd4e46b11da047c9049b"
        );
    }

    #[test]
    fn test_serialization() {
        let abi = FunctionABI {
            name: "transfer".to_string(),
            inputs: vec![
                ABIToken {
                    _name: "to".to_string(),
                    internal_type: "address".to_string(),
                },
                ABIToken {
                    _name: "value".to_string(),
                    internal_type: "uint256".to_string(),
                },
            ],
            outputs: vec![],
            _state_mutability: "nonpayable".to_string(),
            _constant: false,
        };
        println!("{}", serde_json::to_string(&abi).unwrap());
        let mut serializer = serde_json::Serializer::new(Vec::new());
        abi.serialize_dgraph(&mut serializer).unwrap();
        println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
    }

    #[test]
    fn test_unresolved_signature() {
        let abi = FunctionABI {
            name: "Unresolved_f8b2cb4f".to_string(),
            inputs: vec![],
            outputs: vec![],
            _state_mutability: "nonpayable".to_string(),
            _constant: false,
        };
        let mut serializer = serde_json::Serializer::new(Vec::new());
        abi.serialize_dgraph(&mut serializer).unwrap();
        println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
        assert_eq!(
            format!("{:?}", abi.get_signature_hash()),
            "0xc0d559150c15862e872a031a8e11f466df4b16d14e736187f2e7fb162060f9d0"
        );
    }
}

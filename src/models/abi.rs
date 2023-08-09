use super::{error::ErrorABI, event::EventABI, function::FunctionABI};
use primitive_types::H256;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractABI {
    pub nodes: Vec<ABIStructure>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum ABIStructure {
    #[serde(rename = "function")]
    Function(FunctionABI),
    #[serde(rename = "error")]
    Error(ErrorABI),
    #[serde(rename = "event")]
    Event(EventABI),
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ABIToken {
    #[serde(rename = "name")]
    pub _name: String,
    #[serde(rename = "internalType")]
    pub internal_type: String,
}

impl ABIStructure {
    pub fn get_signature_hash(&self) -> H256 {
        match self {
            ABIStructure::Function(f) => f.get_signature_hash(),
            ABIStructure::Event(e) => e.get_signature_hash(),
            ABIStructure::Error(e) => e.get_signature_hash(),
        }
    }

    #[allow(dead_code)]
    pub fn get_input_types(&self) -> String {
        match self {
            ABIStructure::Function(f) => f.get_input_types(),
            ABIStructure::Event(e) => e.get_input_types(),
            ABIStructure::Error(e) => e.get_input_types(),
        }
    }
}

impl PartialEq for ContractABI {
    fn eq(&self, other: &Self) -> bool {
        if self.nodes.len() != other.nodes.len() {
            return false;
        }
        // order doesn't matter in ABI vector
        self.nodes
            .iter()
            .all(|node| other.nodes.iter().any(|n| n == node))
    }
}
impl Eq for ContractABI {}

impl ContractABI {
    pub(crate) fn new(abi: Vec<ABIStructure>) -> Self {
        Self { nodes: abi }
    }

    pub(crate) fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let abi: Vec<ABIStructure> = serde_json::from_str(json)?;
        Ok(Self::new(abi))
    }

    pub(crate) fn _resolve(
        &mut self,
        functions: &HashMap<String, Vec<String>>,
        events: &HashMap<String, Vec<String>>,
        errors: &HashMap<String, Vec<String>>,
    ) {
        for node in &mut self.nodes {
            match node {
                ABIStructure::Function(f) => {
                    let sig = f.name.split('_').nth(1);
                    if let Some(sig) = sig {
                        if let Some(name) = functions.get(sig) {
                            f.name = name.get(0).unwrap_or(&f.name).to_string();
                        }
                    }
                }
                ABIStructure::Event(e) => {
                    let sig = e.name.split('_').nth(1);
                    if let Some(sig) = sig {
                        if let Some(name) = events.get(sig) {
                            e.name = name.get(0).unwrap_or(&e.name).to_string();
                        }
                    }
                }
                ABIStructure::Error(e) => {
                    let sig = e.name.split('_').nth(1);
                    if let Some(sig) = sig {
                        if let Some(name) = errors.get(sig) {
                            e.name = name.get(0).unwrap_or(&e.name).to_string();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn get_function_by_signature(
        &self,
        name: &str,
        inputs: &str,
    ) -> Option<&FunctionABI> {
        self.nodes
            .iter()
            .filter_map(|node| {
                if let ABIStructure::Function(f) = node {
                    if f.name == name && f.get_input_types() == inputs {
                        return Some(f);
                    }
                }
                None
            })
            .next()
    }

    pub(crate) fn _get_event(&self, name: &str) -> Option<&EventABI> {
        for node in &self.nodes {
            if let ABIStructure::Event(e) = node {
                if e.name == name {
                    return Some(e);
                }
            }
        }
        None
    }

    pub(crate) fn _get_error(&self, name: &str) -> Option<&ErrorABI> {
        for node in &self.nodes {
            if let ABIStructure::Error(e) = node {
                if e.name == name {
                    return Some(e);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_serialization() {
        let function = FunctionABI {
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

        let error = ErrorABI {
            name: "InsufficientBalance".to_string(),
            inputs: vec![],
        };

        let event = EventABI {
            name: "Transfer".to_string(),
            inputs: vec![
                ABIToken {
                    _name: "from".to_string(),
                    internal_type: "address".to_string(),
                },
                ABIToken {
                    _name: "to".to_string(),
                    internal_type: "address".to_string(),
                },
                ABIToken {
                    _name: "value".to_string(),
                    internal_type: "uint256".to_string(),
                },
            ],
        };

        let nodes = vec![
            ABIStructure::Function(function),
            ABIStructure::Error(error),
            ABIStructure::Event(event),
        ];

        let abi = ContractABI::new(nodes);

        // serialize abi
        let encoded = serde_json::to_string(&abi).unwrap();
        println!("{:?}", encoded);
        // deserialize abi
        let decoded: ContractABI = serde_json::from_str(&encoded).unwrap();

        println!("{:?}", decoded);
    }
}

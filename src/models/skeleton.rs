use super::{abi::ContractABI, SerializeDgraph};
use crate::models::abi::ABIStructure;
use dgraph_tonic::{IClient, Mutate};
use ethers::utils::keccak256;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

#[derive(Debug, Clone)]
pub struct Skeleton {
    bytecode: ethers::types::Bytes,
    abi: Option<ContractABI>,
    failed_decompilation: bool,
}

impl Skeleton {
    pub fn new(bytecode: ethers::types::Bytes) -> Self {
        Self {
            bytecode,
            abi: None,
            failed_decompilation: false,
        }
    }

    pub fn set_failed_decompilation(&mut self, failed: bool) {
        self.failed_decompilation = failed;
    }

    pub fn set_abi(&mut self, abi: ContractABI) {
        self.abi = Some(abi);
    }

    pub fn get_abi(&self) -> &Option<ContractABI> {
        &self.abi
    }

    /// How much the contract is ERC20 compliant
    /// Returns:
    /// - how many functions of the standard are present (1 to 6)
    fn erc20_compliancy(&self) -> u8 {
        let mut compliance: u8 = 0;
        if self.abi.is_none() {
            return compliance;
        }
        let abi = self.abi.as_ref().unwrap();
        if abi.get_function_by_signature("totalSupply", "").is_some() {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("balanceOf", "address")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("transfer", "address,uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("transferFrom", "address,address,uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("approve", "address,uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("allowance", "address,address")
            .is_some()
        {
            compliance += 1;
        }
        compliance
    }

    /// Returns true if the contract is ERC20 compliant, false otherwise
    /// It checks if the contract has at least 5 functions of the standard and if it has the transfer function
    // pub(crate) fn is_erc20(&self) -> bool {
    //     self.erc20_compliancy() >= 5
    //         && self.abi.as_ref().unwrap().get_function_by_signature("transfer", "address,uint256").is_some()
    // }

    // /// Returns true if the contract is ERC721 compliant, false otherwise
    // /// It checks if the contract has at least 8 functions of the standard
    // pub(crate) fn is_erc721(&self) -> bool {
    //     self.erc721_compliancy() >= 8
    // }

    /// How much the contract is ERC721 compliant
    /// Parameters:
    /// - `compliance`: how many functions must be present to be considered compliant (1 to 9)
    fn erc721_compliancy(&self) -> u8 {
        let mut compliance: u8 = 0;
        if self.abi.is_none() {
            return compliance;
        }
        let abi = self.abi.as_ref().unwrap();
        if abi
            .get_function_by_signature("balanceOf", "address")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("ownerOf", "uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("safeTransferFrom", "address,address,uint256,bytes")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("safeTransferFrom", "address,address,uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("transferFrom", "address,address,uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("approve", "address,uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("setApprovalForAll", "address,bool")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("getApproved", "uint256")
            .is_some()
        {
            compliance += 1;
        }
        if abi
            .get_function_by_signature("isApprovedForAll", "address,address")
            .is_some()
        {
            compliance += 1;
        }
        compliance
    }

    /// Insert skeleton to dgraph
    /// Check of duplicate bytecode are done before, be sure to check it before calling this function
    /// It returns the uid of the inserted node
    pub async fn upsert<S: IClient>(
        &self,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<String, anyhow::Error> {
        let bytecode = self.bytecode.to_string();
        let failed_decompilation = self.failed_decompilation;

        let abi_queries = if self.abi.is_some() {
            self.abi
                .as_ref()
                .unwrap()
                .nodes
                .iter()
                .enumerate()
                .map(|(i, abi)| match abi {
                    ABIStructure::Function(f) => (
                        format!(
                            r#"var(func: eq(Function.signature, "{:?}")){{ f{} as uid }}"#,
                            f.get_signature_hash(),
                            i
                        ),
                        format!(
                            r#"uid(Skeleton) <Skeleton.functions> uid(f{i}) .
                        uid(f{i}) <dgraph.type> "Function" .
                        uid(f{i}) <Function.signature> "{sig}" .
                        uid(f{i}) <Function.name> "{name}" .
                        uid(f{i}) <Function.inputs> "{inputs}" .
                        uid(f{i}) <Function.outputs> "{outputs}" .
                        "#,
                            i = i,
                            sig = format!("{:?}", f.get_signature_hash()),
                            name = f.name,
                            inputs = f.get_input_types(),
                            outputs = f.get_output_types()
                        ),
                    ),
                    ABIStructure::Event(e) => (
                        format!(
                            r#"var(func: eq(Event.signature, "{:?}")){{ e{} as uid }}"#,
                            e.get_signature_hash(),
                            i
                        ),
                        format!(
                            r#"uid(Skeleton) <Skeleton.events> uid(e{i}) .
                        uid(e{i}) <dgraph.type> "Event" .
                        uid(e{i}) <Event.signature> "{sig}" .
                        uid(e{i}) <Event.name> "{name}" .
                        uid(e{i}) <Event.inputs> "{inputs}" .
                        "#,
                            i = i,
                            sig = format!("{:?}", e.get_signature_hash()),
                            name = e.name,
                            inputs = e.get_input_types(),
                        ),
                    ),
                    ABIStructure::Error(e) => (
                        format!(
                            r#"var(func: eq(Event.signature, "{:?}")){{ err{} as uid }}"#,
                            e.get_signature_hash(),
                            i
                        ),
                        format!(
                            r#"uid(Skeleton) <Skeleton.errors> uid(err{i}) .
                        uid(err{i}) <dgraph.type> "Error" .
                        uid(err{i}) <Error.signature> "{sig}" .
                        uid(err{i}) <Error.name> "{name}" .
                        uid(err{i}) <Error.inputs> "{inputs}" .
                        "#,
                            i = i,
                            sig = format!("{:?}", e.get_signature_hash()),
                            name = e.name,
                            inputs = e.get_input_types(),
                        ),
                    ),
                })
                .collect()
        } else {
            Vec::new()
        };

        let query = format!(
            r#"
        query {{
            Skeleton as skeleton(func: eq(Skeleton.bytecode, "{}")){{ uid }}
            {}
        }}"#,
            bytecode,
            abi_queries
                .iter()
                .map(|(q, _)| q.clone())
                .collect::<Vec<String>>()
                .join("\n")
        );

        let set = format!(
            r#"
        uid(Skeleton) <Skeleton.bytecode> "{}" .
        uid(Skeleton) <Skeleton.failed_decompilation> "{}" .
        uid(Skeleton) <dgraph.type> "Skeleton" .
        {}"#,
            bytecode,
            failed_decompilation,
            abi_queries
                .iter()
                .map(|(_, s)| s.clone())
                .collect::<Vec<String>>()
                .join("\n")
        );

        // Perform the upsert
        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_set_nquads(set);
        let mut txn = dgraph_client.new_mutated_txn();
        let res = txn.upsert(query, mu).await?;
        txn.commit().await?;

        #[derive(Deserialize, Debug)]
        struct QueryItem {
            uid: String,
        }

        #[derive(Deserialize, Debug)]
        struct Response {
            skeleton: Vec<QueryItem>,
        }

        let res_parsed: Response = serde_json::from_slice(&res.json)?;

        let uid = if res_parsed.skeleton.is_empty() {
            res.uids.get("uid(Skeleton)").unwrap().clone()
        } else {
            res_parsed.skeleton.get(0).unwrap().uid.clone()
        };

        Ok(uid)
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Uid {
            uid: String,
        }
        let mut state = serializer.serialize_struct("Skeleton", 5)?;
        state.serialize_field("dgraph.type", "Skeleton")?;
        let uid = format!(
            "_:sk{}",
            ethers::types::Bytes::from(keccak256(&self.bytecode))
        );
        state.serialize_field("uid", &uid)?;
        state.serialize_field("Skeleton.bytecode", &self.bytecode)?;
        state.serialize_field("Skeleton.failed_decompilation", &self.failed_decompilation)?;
        state.serialize_field("Skeleton.erc20_compliancy", &self.erc20_compliancy())?;
        state.serialize_field("Skeleton.erc721_compliancy", &self.erc721_compliancy())?;
        let mut functions = Vec::new();
        let mut events = Vec::new();
        let mut errors = Vec::new();
        if self.abi.is_some() {
            let abi = self.abi.as_ref().unwrap();
            for node in &abi.nodes {
                let sig_hash = node.get_signature_hash();
                match node {
                    ABIStructure::Function(_) => functions.push(Uid {
                        uid: format!("_:{:?}", sig_hash),
                    }),
                    ABIStructure::Event(_) => events.push(Uid {
                        uid: format!("_:{:?}", sig_hash),
                    }),
                    ABIStructure::Error(_) => errors.push(Uid {
                        uid: format!("_:{:?}", sig_hash),
                    }),
                }
            }
        }
        state.serialize_field("Skeleton.functions", &functions)?;
        state.serialize_field("Skeleton.events", &events)?;
        state.serialize_field("Skeleton.errors", &errors)?;
        state.end()
    }
}

impl SerializeDgraph for Skeleton {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        models::skeleton::Skeleton,
        utils::{decompile::decompile, metadata::separate_metadata, skeleton::extract_skeleton},
    };
    use ethabi::Address;
    use ethers::providers::Middleware;
    use ethers::providers::Provider;
    use std::{str::FromStr, sync::Arc};

    #[tokio::test]
    #[ignore]
    async fn test_skeleton_upsert() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");

        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let address = "0x0000000000ffe8b47b3e2130213b802212439497";
        let deployed_code = eth_client
            .get_code(Address::from_str(address).unwrap(), None)
            .await
            .unwrap();
        let separated = separate_metadata(&deployed_code);
        let skeleton = if separated.is_some() {
            let (runtime, _) = separated.unwrap();
            extract_skeleton(runtime)
        } else {
            extract_skeleton(&deployed_code)
        };
        let mut skeleton = Skeleton::new(skeleton);
        let abi = decompile(&Address::from_str(address).unwrap(), &deployed_code, 5000).await;

        match abi {
            Ok(abi) => skeleton.set_abi(abi),
            Err(_) => skeleton.set_failed_decompilation(true),
        }

        let uid = skeleton.upsert(&dgraph).await.unwrap();

        println!("Upserted skeleton with uid {}", uid);
    }
}

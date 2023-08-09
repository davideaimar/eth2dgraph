use super::trace::Traces;
use super::SerializeDgraph;
use crate::utils::metadata::{analyze_metadata, separate_metadata, Metadata};
use crate::utils::skeleton::extract_skeleton;
use dgraph_tonic::IClient;
use dgraph_tonic::Mutate;
use ethabi::{ethereum_types::U64, Address};
use ethers::providers::Middleware;
use ethers::types::Trace;
use ethers::types::TxHash;
use ethers::utils::keccak256;
use ethers_core::abi::Abi;
use glob::glob_with;
use glob::MatchOptions;
use primitive_types::H256;
use serde::Deserialize;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractDeployment {
    failed: bool,
    contract_address: Address,
    creator: Address,
    tx_hash: TxHash,
    block_number: U64,
    creation_code: ethers::types::Bytes,
    deployed_code: ethers::types::Bytes,
    skeleton: ethers::types::Bytes,
    metadata: Option<Metadata>,
    verified_source: Option<String>,
    name: Option<String>,
}

impl From<Traces> for Vec<ContractDeployment> {
    fn from(traces: Traces) -> Self {
        let mut deployments = Vec::new();
        for trace in traces.0 {
            if let Ok(deployment) = ContractDeployment::try_from(trace) {
                deployments.push(deployment);
            }
        }
        deployments
    }
}

impl TryFrom<Trace> for ContractDeployment {
    type Error = ();

    fn try_from(trace: Trace) -> Result<Self, Self::Error> {
        if trace.result.is_none() {
            return Err(());
        }
        let failed = trace.error.is_some();
        let block_number = trace.block_number;
        let tx_hash = trace.transaction_hash.unwrap();
        let res = trace.result.unwrap();
        let (contract_address, deployed_code) = match res {
            ethers::types::Res::Call(_) => return Err(()),
            ethers::types::Res::Create(r) => (r.address, r.code),
            ethers::types::Res::None => return Err(()),
        };
        let (creation_code, creator) = match trace.action {
            ethers::types::Action::Call(_) => return Err(()),
            ethers::types::Action::Create(a) => (a.init, a.from),
            ethers::types::Action::Suicide(_) => return Err(()),
            ethers::types::Action::Reward(_) => return Err(()),
        };

        let separated = separate_metadata(&deployed_code);

        let (skeleton, metadata) = if separated.is_some() {
            let (runtime, metadata) = separated.unwrap();
            (extract_skeleton(runtime), analyze_metadata(metadata))
        } else {
            let skeleton = extract_skeleton(&deployed_code);
            (skeleton, None)
        };
        Ok(Self {
            failed,
            contract_address,
            creator,
            tx_hash,
            block_number: block_number.into(),
            creation_code,
            deployed_code,
            skeleton,
            metadata,
            verified_source: None,
            name: None,
        })
    }
}

impl ContractDeployment {
    pub fn contract_address(&self) -> Address {
        self.contract_address
    }

    pub fn deployed_code(&self) -> &ethers::types::Bytes {
        &self.deployed_code
    }

    pub fn creation_code(&self) -> &ethers::types::Bytes {
        &self.creation_code
    }

    pub fn skeleton_hash(&self) -> H256 {
        H256::from(keccak256(&self.skeleton))
    }

    pub fn skeleton(&self) -> &ethers::types::Bytes {
        &self.skeleton
    }

    pub async fn resolve_name<T>(&mut self, eth_client: Arc<T>) -> bool
    where
        T: Middleware,
    {
        let abi: Abi = serde_json::from_str(
            r#"[
            {
            "constant": true,
            "inputs": [],
            "name": "name",
            "outputs": [
                {
                    "name": "",
                    "type": "string"
                }
            ],
            "payable": false,
            "stateMutability": "view",
            "type": "function"
        }
        ]"#,
        )
        .unwrap();

        let contract = ethers::contract::Contract::new(self.contract_address, abi, eth_client);

        let method = contract.method::<_, String>("name", ());
        if method.is_err() {
            return false;
        }
        let name = method.unwrap().call().await;

        if let Ok(name) = name {
            self.name = Some(name);
            return true;
        }

        false
    }

    pub fn check_verification(&mut self, scs_path: &str) {
        // search for the contract in the smart-contract-santuary-ethereum repo, cloned at scs_path
        // if found, read the source code and store it in the struct
        // if not found or error, store None in the struct

        // source of mainet contracts is in scs_path/contracts/mainnet/<first 2 chars of address>/<address>_<name>.<ext>
        let options = MatchOptions {
            case_sensitive: false,
            require_literal_separator: false,
            require_literal_leading_dot: false,
        };
        let stripped_address = format!("{:?}", self.contract_address)
            .to_lowercase()
            .chars()
            .skip(2)
            .collect::<String>();
        let path = Path::new(scs_path)
            .join("contracts")
            .join("mainnet")
            .join(&stripped_address[0..2]);
        let pattern = &format!("{}/{}*", path.display(), stripped_address);

        let mut source_code = None;

        for entry in glob_with(pattern, options).unwrap() {
            if let Ok(path) = entry {
                // read the file
                if let Ok(content) = std::fs::read_to_string(&path) {
                    source_code = Some(content);
                    break;
                }
            }
        }

        self.verified_source = source_code;
    }

    /// Upsert the contract deployment in the graph database
    /// it also manage the skeleton and its decompilation
    pub async fn upsert<S: IClient>(
        &self,
        skeleton_uid: &str,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        // WARNING:
        // Deployments don't have a unique identifier
        // upserting already existing deployment will result in a duplicate
        // This function should be called just after checking if the deployment
        // of a certain block already exists, or after deleting them using Block::upsert_delete_deployments
        let block_no = self.block_number.as_u64();
        let contract_address = format!("{:?}", self.contract_address);
        let creator_address = format!("{:?}", self.creator);
        let creation_code = self.creation_code().to_string();
        let deployed_code = self.deployed_code().to_string();
        let failed_deploy = self.failed;
        let tx_hash = format!("{:?}", self.tx_hash);
        let verified_source = self.verified_source.is_some();
        let verified_source_code = self.verified_source.as_ref();
        let name = self.name.as_ref();
        let (solc, storage_protocol, storage_address, experimental) = if self.metadata.is_some() {
            let metadata = self.metadata.as_ref().unwrap();
            let solc = if metadata.compiler.is_some() {
                Some(metadata.compiler.as_ref().unwrap())
            } else {
                None
            };
            (
                solc,
                Some(&metadata.storage_protocol),
                Some(&metadata.storage_hash),
                Some(&metadata.experimental),
            )
        } else {
            (None, None, None, None)
        };

        // Query part of the upsert
        let query = format!(
            r#"
            query{{
            var(func: eq(Block.number, {block_no})) {{ Block as uid }}
            var(func: eq(Account.address, "{contract_address}")) {{ Address as uid }}
            var(func: eq(Account.address, "{creator_address}")) {{ Creator as uid }}
            }}
        "#,
            block_no = block_no,
            contract_address = contract_address,
            creator_address = creator_address
        );

        // Mutation part of the upsert
        let mut set = format!(
            r#"
            uid(Block) <Block.number> "{block_no}" .
            uid(Address) <Account.address> "{contract_address}" .
            uid(Creator) <Account.address> "{creator_address}" .

            _:deployment <ContractDeployment.contract> uid(Address) .
            _:deployment <ContractDeployment.creator> uid(Creator) .
            _:deployment <ContractDeployment.block> uid(Block) .
            _:deployment <dgraph.type> "ContractDeployment" .
            _:deployment <ContractDeployment.creation_code> "{creation_code}" .
            _:deployment <ContractDeployment.deployed_code> "{deployed_code}" .
            _:deployment <ContractDeployment.failed> "{failed_deploy}" .
            _:deployment <ContractDeployment.tx_hash> "{tx_hash}" .
            _:deployment <ContractDeployment.verified_source> "{verified_source}" .
            _:deployment <ContractDeployment.skeleton> <{skeleton_uid}> .

        "#,
            block_no = block_no,
            contract_address = contract_address,
            creator_address = creator_address,
            creation_code = creation_code,
            deployed_code = deployed_code,
            failed_deploy = failed_deploy,
            tx_hash = tx_hash,
            verified_source = verified_source,
            skeleton_uid = skeleton_uid
        );

        if name.is_some() {
            set.push_str(&format!(
                r#"
                _:deployment <ContractDeployment.name> "{name}" .
                "#,
                name = name.unwrap()
            ));
        }

        if solc.is_some() {
            set.push_str(&format!(
                r#"
                _:deployment <ContractDeployment.solc_version> "{solc}" .
                "#,
                solc = solc.unwrap()
            ));
        }
        if storage_protocol.is_some() {
            set.push_str(&format!(
                r#"
                _:deployment <ContractDeployment.storage_protocol> "{storage_protocol}" .
                "#,
                storage_protocol = storage_protocol.unwrap()
            ));
        }
        if storage_address.is_some() {
            set.push_str(&format!(
                r#"
                _:deployment <ContractDeployment.storage_address> "{storage_address}" .
                "#,
                storage_address = storage_address.unwrap()
            ));
        }
        if experimental.is_some() {
            set.push_str(&format!(
                r#"
                _:deployment <ContractDeployment.experimental> "{experimental}" .
                "#,
                experimental = experimental.unwrap()
            ));
        }
        if verified_source_code.is_some() {
            let source_code = verified_source_code.unwrap();
            set.push_str(&format!(
                r#"
                _:deployment <ContractDeployment.verified_source_code> "{verified_source_code}" .
                "#,
                verified_source_code = source_code
            ));
        }

        // Perform the upsert
        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_set_nquads(set);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Uid {
            uid: String,
        }
        let mut state = serializer.serialize_struct("ContractDeployment", 7)?;
        state.serialize_field("dgraph.type", &json!(["ContractDeployment"]))?;
        state.serialize_field(
            "ContractDeployment.contract",
            &json!({
                "uid": format!("_:{:?}", self.contract_address),
                "dgraph.type": "Account",
                "Account.address": format!("{:?}", self.contract_address),
                "Account.is_contract": &true,
            }),
        )?;
        state.serialize_field("ContractDeployment.creation_bytecode", self.creation_code())?;
        state.serialize_field("ContractDeployment.deployed_bytecode", self.deployed_code())?;
        state.serialize_field(
            "ContractDeployment.creator",
            &json!({
                "uid": format!("_:{:?}", self.creator),
                "dgraph.type": ["Account"],
                "Account.address": format!("{:?}", self.creator)
            }),
        )?;
        state.serialize_field(
            "ContractDeployment.block",
            &Uid {
                uid: format!("_:{}", self.block_number.as_u64()),
            },
        )?;
        state.serialize_field("ContractDeployment.failed_deploy", &self.failed)?;
        state.serialize_field("ContractDeployment.tx_hash", &self.tx_hash)?;
        let skeleton_key = H256::from(keccak256(&self.skeleton));
        state.serialize_field(
            "ContractDeployment.skeleton",
            &Uid {
                uid: format!("_:sk{:?}", skeleton_key),
            },
        )?;
        let verified = self.verified_source.is_some();
        state.serialize_field("ContractDeployment.verified_source", &verified)?;
        if verified {
            state.serialize_field(
                "ContractDeployment.verified_source_code",
                self.verified_source.as_ref().unwrap(),
            )?;
        }
        if self.name.is_some() {
            state.serialize_field("ContractDeployment.name", self.name.as_ref().unwrap())?;
        }
        if self.metadata.is_some() {
            let metadata = self.metadata.as_ref().unwrap();
            if metadata.compiler.is_some() {
                state.serialize_field(
                    "ContractDeployment.solc_version",
                    metadata.compiler.as_ref().unwrap(),
                )?;
            }
            state.serialize_field(
                "ContractDeployment.storage_protocol",
                &metadata.storage_protocol,
            )?;
            state.serialize_field("ContractDeployment.storage_address", &metadata.storage_hash)?;
            state.serialize_field("ContractDeployment.experimental", &metadata.experimental)?;
        }
        state.end()
    }
}

impl SerializeDgraph for ContractDeployment {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::decompile::decompile;
    use crate::{
        extraction::traces::get_traces,
        models::{block::Block, contract_deployment::ContractDeployment, skeleton::Skeleton},
    };
    use ethers::providers::Provider;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_source_verification() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 16075682;

        let creation_traces = get_traces(block, eth_client).await.unwrap();
        let deployments: Vec<ContractDeployment> = Vec::from(creation_traces);

        assert_eq!(deployments.len(), 1);

        for mut deployment in deployments {
            deployment.check_verification("smart-contract-sanctuary-ethereum");
            assert!(deployment.verified_source.is_some());
            let mut serializer = serde_json::Serializer::new(Vec::new());
            deployment.serialize_dgraph(&mut serializer).unwrap();
            println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_deployment_serialization() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 4719568; // weth creation block

        let creation_traces = get_traces(block, eth_client.clone()).await.unwrap();
        let deployments: Vec<ContractDeployment> = Vec::from(creation_traces);

        for mut deployment in deployments {
            deployment.resolve_name(eth_client.clone()).await;
            let mut serializer = serde_json::Serializer::new(Vec::new());
            deployment.serialize_dgraph(&mut serializer).unwrap();
            println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_deployment_upsert_whole_block() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block = 16100053u64;

        // 1: Delete all deployments in the block
        Block::upsert_delete_deployments(block, &dgraph)
            .await
            .unwrap();
        // 2: Get the deplyments of that block from traces
        let traces = get_traces(block, eth_client).await.unwrap();
        let deployments: Vec<ContractDeployment> = Vec::from(traces);

        // For each deployment:
        for deployment in deployments {
            // 3: Decompile the skeleton
            let decompiled_skeleton = decompile(
                &deployment.contract_address(),
                &deployment.deployed_code(),
                5000,
            )
            .await;

            let mut skeleton = Skeleton::new(deployment.skeleton().clone());

            match decompiled_skeleton {
                Ok(decompiled_skeleton) => {
                    skeleton.set_abi(decompiled_skeleton);
                }
                Err(_) => {
                    skeleton.set_failed_decompilation(true);
                }
            }

            // 4: Upsert the skeleton
            let uid = match skeleton.upsert(&dgraph).await {
                Ok(uid) => uid,
                Err(_) => {
                    println!("Error upserting skeleton: {:?}", skeleton);
                    println!("Continuing...");
                    continue;
                }
            };

            // 5: Upsert the deployment
            deployment.upsert(&uid, &dgraph).await.unwrap();
        }
    }
}

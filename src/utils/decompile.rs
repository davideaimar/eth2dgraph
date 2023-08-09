use std::time::Duration;

use ethabi::Address;

use crate::models::abi::ContractABI;

#[derive(Debug)]
pub enum DecompilationError {
    Timeout,
    FailedToReadABI,
    FailedToParseABI,
}

pub async fn decompile(
    address: &Address,
    bytecode: &ethers::types::Bytes,
    timeout: u64,
) -> Result<ContractABI, DecompilationError> {
    // spawn a new heimdall process to decompile the contract using the async tokio implementation of process
    let mut cmd = tokio::process::Command::new("heimdall")
        .arg("decompile")
        .arg(bytecode.to_string())
        .arg("--default")
        .arg("--output") // output directory
        .arg(format!(".tmp/{}/", address)) // work in .tmp/<contract_address>/ since it's unique
        .stdout(std::process::Stdio::null()) // redirect stdout and stderr to /dev/null
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("Failed to spawn heimdall decompiler.");

    // wait for the process to finish, or kill it after <timeout> milliseconds
    if (tokio::time::timeout(Duration::from_millis(timeout), cmd.wait()).await).is_err() {
        let _ = cmd.kill().await;
        println!("Contract {:?} decompilation timed out", address);
        let _ = tokio::fs::remove_dir_all(format!(".tmp/{}/", address)).await;
        return Err(DecompilationError::Timeout);
    }

    let json = &tokio::fs::read_to_string(format!(".tmp/{}/abi.json", address).as_str()).await;

    if json.is_err() {
        let _ = cmd.kill().await;
        println!("No ABI for {:?}.", address);
        let _ = tokio::fs::remove_dir_all(format!(".tmp/{}/", address)).await;
        return Err(DecompilationError::FailedToReadABI);
    }

    let abi = ContractABI::from_json(json.as_ref().unwrap());

    if abi.is_err() {
        let _ = cmd.kill().await;
        println!(
            "Contract {:?} failed to parse abi.json. Error: {}",
            address,
            abi.err().unwrap()
        );
        let _ = tokio::fs::remove_dir_all(format!(".tmp/{}/", address)).await;
        return Err(DecompilationError::FailedToParseABI);
    }

    // finally delete the directory
    let _ = tokio::fs::remove_dir_all(format!(".tmp/{}/", address)).await;

    Ok(abi.unwrap())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use ethers::providers::Provider;
    use primitive_types::H256;

    use crate::{
        extraction::traces::get_traces,
        models::{abi::ContractABI, contract_deployment::ContractDeployment},
        utils::decompile::decompile,
    };

    #[tokio::test]
    #[ignore]
    async fn test_decompilation_cache_precision() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let mut block = 6000000;
        let to = 6001000;

        let mut matches = 0;
        let mut mismatches = 0;

        let mut skeleton_abis: HashMap<H256, ContractABI> = HashMap::new();

        while block <= to {
            let creation_traces = get_traces(block, eth_client.clone()).await.unwrap();
            let deployments: Vec<ContractDeployment> = Vec::from(creation_traces);

            for deployment in deployments {
                let abi = decompile(
                    &deployment.contract_address(),
                    &deployment.deployed_code(),
                    2000,
                )
                .await;

                if abi.is_err() {
                    continue;
                }

                let skeleton_hash = deployment.skeleton_hash();

                if skeleton_abis.contains_key(&skeleton_hash) {
                    let abi = abi.unwrap();
                    if skeleton_abis.get(&skeleton_hash).unwrap().eq(&abi) {
                        matches += 1;
                    } else {
                        mismatches += 1;
                        println!(
                            "Mismatch: {:?} {:?}",
                            skeleton_abis.get(&skeleton_hash).unwrap(),
                            &abi
                        );
                    }
                } else {
                    skeleton_abis.insert(skeleton_hash, abi.unwrap());
                }
            }

            block += 1;
        }

        println!("Matches: {}", matches);
        println!("Mismatches: {}", mismatches);
    }
}

use super::writer::WriteCommand;
use crate::{
    extraction::{
        blocks::get_block,
        logs::{get_all_logs, get_transfer_from_logs, get_transfer_logs},
        traces::get_traces,
        writer::writer_task,
    },
    models::{
        contract_destruction::ContractDestruction, skeleton::Skeleton, transaction::Transaction,
    },
    utils::decompile::decompile,
    ExtractArgs,
};
use dashmap::DashMap;
use ethers::providers::{Middleware, Provider, RetryClientBuilder};
use primitive_types::H256;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tokio::time::Duration;

pub struct Extractor<T>
where
    T: Middleware,
{
    output_path: String,
    output_size: usize,
    compression_level: u32,
    from_block: u64,
    to_block: u64,
    num_tasks: usize,
    eth_provider: Arc<T>,
    include_tx: bool,
    include_token_transfers: bool,
    include_logs: bool,
    scs_path: Option<String>,
    decompiler_timeout: u64,
    skip_decompilation: bool,
}

impl<T> Extractor<T>
where
    T: Middleware + 'static,
{
    pub fn new(
        eth_provider: T,
        output_path: String,
        output_size: usize,
        compression_level: u32,
        from_block: u64,
        to_block: u64,
        num_tasks: usize,
        include_tx: bool,
        include_token_transfers: bool,
        include_logs: bool,
        scs_path: Option<String>,
        decompiler_timeout: u64,
        skip_decompilation: bool,
    ) -> Self {
        Self {
            output_path,
            output_size,
            compression_level,
            from_block,
            to_block,
            num_tasks,
            eth_provider: Arc::new(eth_provider),
            include_tx,
            include_logs,
            include_token_transfers,
            scs_path,
            decompiler_timeout,
            skip_decompilation,
        }
    }

    async fn extract_at(
        block: u64,
        eth_provider: Arc<T>,
        cnt_total: Arc<AtomicU64>,
        cnt_failed: Arc<AtomicU64>,
        writer: Sender<WriteCommand>,
        skeletons: Arc<DashMap<H256, AtomicU8>>,
        include_tx: bool,
        include_token_transfers: bool,
        include_logs: bool,
        scs_path: Option<String>,
        decompiler_timeout: u64,
        skip_decompilation: bool,
    ) {
        let c = eth_provider.clone();
        let block_data = get_block(block, c);

        let c = eth_provider.clone();
        let traces = get_traces(block, c);

        let (block_data, logs, traces) = if include_token_transfers || include_logs {
            let c = eth_provider.clone();

            let (block_data, logs, traces) = if include_token_transfers && !include_logs {
                tokio::join!(block_data, get_transfer_logs(block, c), traces)
            } else {
                tokio::join!(block_data, get_all_logs(block, c), traces)
            };

            if block_data.is_err() || logs.is_err() || traces.is_err() {
                println!("Network error while processing block {}", block);
                return;
            }

            // can unwrap now

            let block_data = block_data.unwrap();
            let logs = logs.unwrap();
            let traces = traces.unwrap();

            (block_data, logs, traces)
        } else {
            // don't need logs if we don't include token transfers

            let (block_data, traces) = tokio::join!(block_data, traces);

            if block_data.is_err() || traces.is_err() {
                println!("Network error while processing block {}", block);
                return;
            }

            // can unwrap now

            let block_data = block_data.unwrap();
            let traces = traces.unwrap();

            (block_data, Vec::new(), traces)
        };

        if block_data.is_none() {
            println!("Block {} not found", block);
            return;
        }

        let block_data = block_data.unwrap();
        let destructions: Vec<ContractDestruction> = Vec::from(&traces);
        let deployments = Vec::from(traces);

        println!(
            "Block {} discovered with {} deploys, {} destructions.",
            block,
            deployments.len(),
            destructions.len()
        );

        for mut deployment in deployments {
            // extract abi of related skeleton and check for verification

            // check for verification
            if scs_path.is_some() {
                deployment.check_verification(scs_path.as_ref().unwrap());
            }

            // resolve name
            deployment.resolve_name(eth_provider.clone()).await;

            let skeleton_hash = deployment.skeleton_hash();

            if skip_decompilation {
                // just store skeleton without decompiling
                let already_wrote = skeletons.get(&skeleton_hash);
                if already_wrote.is_none() {
                    drop(already_wrote);
                    // newly discovered skeleton
                    // just store it without performing decompilation
                    skeletons.insert(skeleton_hash, AtomicU8::new(1));
                    let skeleton = Skeleton::new(deployment.skeleton().clone());
                    writer.send(WriteCommand::Skeleton(skeleton)).await.unwrap();
                }
            } else {
                // Caching logic:
                // 1 - check if skeleton is already discovered, if yes skip decompilation
                // 2 - if not, try to decompile up to 10 times
                // 3 - if decompilation fails more than 10 times, skip decompilation

                // Implementation:
                // - skeletons are stored in a concurrent DashMap<H256, AtomicU8>
                // - the key is the skeleton hash
                // - the value is an AtomicU8 that stores the number of times the skeleton has failed to decompile
                // - the value is initialized to 1 when the skeleton is discovered
                // - the value is incremented by 1 every time the skeleton fails to decompile
                // - the value is set to 0 when the skeleton is successfully decompiled

                // Things to avoid:
                // - keep lock during decompilation -> would slow down the process

                let cached_value = skeletons.entry(skeleton_hash).or_insert(AtomicU8::new(1));

                match cached_value.value().load(Ordering::Relaxed) {
                    0 => {
                        // skeleton already discovered and succesfully decompiled
                        // skip decompilation
                        drop(cached_value);
                        println!("Skeleton already discovered and decompiled");
                    }
                    1..=10 => {
                        // must be decompiled

                        // increment attempt counter, if not 0
                        let _ = cached_value.value().fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| if x == 0 { None } else { Some(x + 1) } );
                        drop(cached_value);

                        // perform decompilation
                        let mut skeleton = Skeleton::new(deployment.skeleton().clone());
                        let abi = decompile(
                            &deployment.contract_address(),
                            &deployment.deployed_code(),
                            decompiler_timeout,
                        )
                        .await;

                        if abi.is_ok() {
                            // decompilation successful
                            skeleton.set_abi(abi.unwrap());
                            skeleton.set_failed_decompilation(false);
                            skeletons.get(&skeleton_hash).unwrap().store(0, Ordering::Relaxed);
                        } else {
                            // decompilation failed
                            // increment attempt counter
                            cnt_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }

                        // store skeleton
                        writer.send(WriteCommand::Skeleton(skeleton)).await.unwrap();
                    }
                    _ => {
                        // skeleton already discovered and failed more than 10 times to decompile
                        // skip decompilation
                        drop(cached_value);
                        println!("Skeleton already discovered and failed more than 10 times to decompile");
                    }
                };
            }

            cnt_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            writer
                .send(WriteCommand::ContractDeployment(deployment))
                .await
                .unwrap();
        }

        for destruction in destructions {
            writer
                .send(WriteCommand::ContractDestruction(destruction))
                .await
                .unwrap();
        }

        if include_token_transfers {
            let transfers = get_transfer_from_logs(&logs);

            for transfer in transfers {
                writer.send(WriteCommand::Transfer(transfer)).await.unwrap();
            }
        }

        if include_logs {
            for log in logs {
                writer.send(WriteCommand::Log(log.into())).await.unwrap();
            }
        }

        // store transactions
        if include_tx {
            for tx in block_data.transactions.iter() {
                let tx: Transaction = tx.clone().into(); // TODO: check if clone is necessary
                writer.send(WriteCommand::Transaction(tx)).await.unwrap();
            }
        }

        // store block data
        writer.send(WriteCommand::Block(block_data)).await.unwrap();

        println!("Block {} processed", block);
    }

    pub async fn run(self, _sender: Sender<()>, mut receiver: Receiver<()>) -> (u64, u64, u64) {
        let num_tasks = if self.num_tasks == 0 {
            5 * num_cpus::get()
        } else {
            self.num_tasks
        };

        println!("Using {} jobs", num_tasks);

        // create output folders if they don't exists
        if !Path::new(&self.output_path).exists() {
            tokio::try_join!(
                tokio::fs::create_dir_all(&self.output_path),
                tokio::fs::create_dir_all(format!("{}/static/skeletons/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/static/events/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/static/functions/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/static/errors/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/static/blocks/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/static/deployments/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/static/destructions/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/dynamic/transactions/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/dynamic/transfers/", &self.output_path)),
                tokio::fs::create_dir_all(format!("{}/dynamic/logs/", &self.output_path)),
            )
            .unwrap();
        }

        // counters to keep track of the progress
        let cnt_total = Arc::new(AtomicU64::new(0));
        let cnt_failed = Arc::new(AtomicU64::new(0));

        // shared hashmap to access the list of already processed skeletons
        // the key is the the skeleton's bytecode hash,
        // the value is a u8 indicating how many times the decompilation failed, if it's 0 the skeleton was successfully decompiled
        let skeletons: Arc<DashMap<H256, AtomicU8>> = Arc::new(DashMap::new());

        // the semaphore is used to limit the number of concurrent tasks, otherwise the system
        // would spawn millions of tasks. The semaphore allows spawning at max <num_tasks> tasks in parallel.
        let semaphore = Arc::new(Semaphore::new(num_tasks));

        // spawn writer task
        let (writer, writer_receiver) = tokio::sync::mpsc::channel(10000);
        let output = self.output_path.to_string();
        let output_size = self.output_size;
        let compression_level = self.compression_level;
        let writer_handle = tokio::spawn(async move {
            writer_task(&output, writer_receiver, output_size, compression_level).await;
        });

        println!(
            "Processing blocks from {} to {}",
            &self.from_block, &self.to_block
        );

        let mut block = self.from_block;
        while block <= self.to_block {
            // acquire a permit from the semaphore, this will block if the semaphore is full
            // to avoid spawning too many tasks.
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let c = self.eth_provider.clone();
            let cnt_failed = cnt_failed.clone(); // clone the counter to pass it to the task
            let cnt_total = cnt_total.clone(); // clone the counter to pass it to the task
            let w = writer.clone();
            let s = skeletons.clone();
            let scs = self.scs_path.clone();
            tokio::spawn(async move {
                Self::extract_at(
                    block,
                    c,
                    cnt_total,
                    cnt_failed,
                    w,
                    s,
                    self.include_tx,
                    self.include_token_transfers,
                    self.include_logs,
                    scs,
                    self.decompiler_timeout,
                    self.skip_decompilation,
                )
                .await;
                drop(permit); // release the permit
            });
            block += 1;
            if receiver.try_recv().is_ok() {
                break;
            }
        }

        block -= 1;

        // Wait for all the tasks to finish acquiring all the permits, this will implicitly wait
        // for all the tasks to finish. Otherwise the program would exit before all the tasks
        // are finished. I did it this way to avoid collecting all the handles (potentially millions) in a vector and
        // waiting for all of them to finish.
        let _ = semaphore.acquire_many(num_tasks as u32).await;

        drop(writer); // close the writer channel, this will cause the writer task to finish

        // wait for the writer task to finish, it can take a while since it's compressing the output
        let _ = writer_handle.await;

        let _ = tokio::fs::remove_dir(".tmp").await;

        (
            cnt_total.load(std::sync::atomic::Ordering::Relaxed),
            cnt_failed.load(std::sync::atomic::Ordering::Relaxed),
            block,
        )
    }
}

pub async fn run_extraction(args: ExtractArgs) {
    let now = std::time::Instant::now();

    let client = RetryClientBuilder::default()
        .rate_limit_retries(10)
        .timeout_retries(5)
        .initial_backoff(Duration::from_millis(500))
        .build(
            ethers::providers::Http::new(reqwest::Url::parse(&args.endpoint).unwrap()),
            Box::<ethers::providers::HttpRateLimitRetryPolicy>::default(),
        );

    let extractor = Extractor::new(
        Provider::new(client),
        args.output_path,
        args.size_output,
        args.compression_level,
        args.from_block,
        args.to_block,
        args.num_tasks,
        args.include_tx,
        args.include_transfers,
        args.include_logs,
        args.scs_path,
        args.decompiler_timeout,
        args.skip_decompilation,
    );

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::channel::<()>(1);
    let (stop_send, stop_recv) = tokio::sync::mpsc::channel::<()>(1);

    let jh = tokio::spawn(async move { extractor.run(shutdown_send, stop_recv).await });

    let (total, failed, last_block) = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            stop_send.send(()).await.unwrap();
            jh.await.unwrap()
        },
        _ = shutdown_recv.recv() => {
            jh.await.unwrap()
        },
    };

    println!(
        "Analysis completed! Extracted blocks from {} to {}",
        args.from_block, last_block
    );
    println!("Total: {} contracts", total);
    if total > 0 {
        println!("Failed: {} contracts", failed);
        println!("Success ratio: {}", (total - failed) as f64 / total as f64);
    }

    let elapsed = now.elapsed();

    println!("Elapsed: {:?}", elapsed);
    println!("Contracts/sec: {}", total as f64 / elapsed.as_secs_f64());

    if last_block - args.from_block > 0 {
        println!(
            "Blocks/sec: {}",
            (last_block - args.from_block) as f64 / elapsed.as_secs_f64()
        );
    }
}

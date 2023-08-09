mod analysys;
mod extraction;
mod models;
mod utils;

use crate::analysys::lifetimes::analyse_lifetimes;
use crate::analysys::similarities::find_similar_skeletons;
use clap::{Args, Parser, Subcommand};
use extraction::{extract::run_extraction, stream::run_stream_extraction};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Extract data in Dgraph format, ready to be loaded with the bulk loader
    Extract(ExtractArgs),
    /// Stream data from node to Dgraph
    Stream(StreamDgraphArgs),
    /// Analyse smart contracts
    Analyse(AnalyseArgs),
}

#[derive(Debug, Args, Clone)]
#[command(args_conflicts_with_subcommands = true)]
pub struct StreamDgraphArgs {
    /// Ethereum node to connect to, with websocket scheme
    #[arg(short, long, default_value = "ws://localhost:8545")]
    endpoint: String,
    /// Dgraph GRPC endpoint
    #[arg(short, long, default_value = "http://localhost:9080")]
    dgraph: String,
    /// Include transactions
    #[arg(long, default_value_t = false)]
    include_tx: bool,
    /// Include token transfers
    #[arg(long, default_value_t = false)]
    include_tokens: bool,
    /// Include logs
    #[arg(long, default_value_t = false)]
    include_logs: bool,
    /// Decompiler timeout in milliseconds
    #[arg(long, default_value_t = 5000)]
    decompiler_timeout: u64,
    /// Skip syncronization from last indexed block in Dgraph, just get live blocks
    #[arg(long, default_value_t = false)]
    no_sync: bool,
    /// Number of Tokio tasks run in parallel
    #[arg(short, long, default_value = "1")]
    num_jobs: usize,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct ExtractArgs {
    /// RPC endpoint to connect to
    #[arg(short, long, default_value = "http://localhost:8545")]
    endpoint: String,
    /// Output path
    #[arg(short, long, default_value = "./extracted")]
    output_path: String,
    /// From block
    #[arg(short, long)]
    from_block: u64,
    /// To block
    #[arg(short, long)]
    to_block: u64,
    /// Number of Tokio tasks ran in parallel
    #[arg(short, long, default_value = "0")]
    num_tasks: usize,
    /// Include transactions
    #[arg(long, default_value_t = false)]
    include_tx: bool,
    /// Include token transfers
    #[arg(long, default_value_t = false)]
    include_transfers: bool,
    /// Include all logs
    #[arg(long, default_value_t = false)]
    include_logs: bool,
    /// smart-contract-sanctuary-ethereum root path
    #[arg(short, long)]
    scs_path: Option<String>,
    /// Max size in RAM of output files before they're flushed and compressed to disk, in KB
    #[arg(long, default_value_t = 8192)]
    size_output: usize,
    /// Compression level of output, from 0 to 9
    #[arg(long, default_value_t = 6)]
    compression_level: u32,
    /// Decompiler timeout in milliseconds
    #[arg(long, default_value_t = 5000)]
    decompiler_timeout: u64,
    /// Skip the extraction of the ABI with heimdall
    #[arg(long, default_value_t = false)]
    skip_decompilation: bool,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
struct AnalyseArgs {
    #[command(subcommand)]
    command: AnalyseCommands,
}

#[derive(Debug, Subcommand)]
pub enum AnalyseCommands {
    Similarities {
        /// Dgraph GRPC endpoint
        #[arg(short, long, default_value = "http://localhost:9080")]
        endpoint: String,
        /// Output file
        #[arg(short, long)]
        output_file: String,
        /// Contract address to calculate the similarities for
        #[arg(short, long)]
        address: Option<String>,
        /// Calculate interface similarity
        #[arg(long, default_value_t = true)]
        interface_sim: bool,
        /// Minimum interface similarity threshold (0.0-1.0) over which similarity is stored
        #[arg(long, default_value_t = 0.75)]
        interface_threshold: f64,
        /// Calculate cosine similarity
        #[arg(long, default_value_t = false)]
        cosine_sim: bool,
        /// Minimum cosine similarity threshold (0.0-1.0) over which similarity is stored
        #[arg(long, default_value_t = 0.95)]
        cosine_threshold: f64,
        /// Length of N-grams to use for cosine similarity
        #[arg(long, default_value_t = 5)]
        ngram_length: u8,
    },
    Lifetimes {
        /// Dgraph GRPC endpoint
        #[arg(short, long, default_value = "http://localhost:9080")]
        endpoint: String,
        /// Output path
        #[arg(short, long)]
        output_path: String,
        /// Cache file to use
        #[arg(short, long)]
        cache_file: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Extract(mut extract_args) => {
            if extract_args.num_tasks == 0 {
                extract_args.num_tasks = 5 * num_cpus::get(); // optimal number from benchmarks
            }
            if (extract_args.include_tx || extract_args.include_transfers)
                && (extract_args.to_block - extract_args.from_block) > 1e6 as u64
            {
                println!("WARNING: Extracting transactions and/or token transfers for a large number of blocks may produce a large number of files");
            }
            if extract_args.compression_level > 9 {
                panic!("Compression level must be between 0 and 9");
            }
            // create the Tokio runtime and run the extraction
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    run_extraction(extract_args).await;
                });
        }
        Commands::Analyse(analyse) => match analyse.command {
            AnalyseCommands::Similarities {
                endpoint,
                output_file,
                address,
                interface_sim,
                interface_threshold,
                cosine_sim,
                cosine_threshold,
                ngram_length,
            } => {
                if interface_threshold < 0.0 || interface_threshold > 1.0 {
                    panic!("Interface similarity threshold must be between 0.0 and 1.0");
                }
                if cosine_threshold < 0.0 || cosine_threshold > 1.0 {
                    panic!("Cosine similarity threshold must be between 0.0 and 1.0");
                }
                if cosine_threshold < 0.9 {
                    println!("WARNING: Cosine similarity threshold is low, this may result in a large number of stored similarities");
                }
                if address.is_none() {
                    println!("WARNING: No contract address specified, all contracts will be analysed, this may take a long time");
                }
                if cosine_sim && ngram_length < 2 {
                    panic!("N-gram length must be at least 2");
                }
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        find_similar_skeletons(
                            &endpoint,
                            &output_file,
                            address,
                            interface_sim,
                            interface_threshold,
                            cosine_sim,
                            cosine_threshold,
                            ngram_length,
                        )
                        .await;
                    });
            }
            AnalyseCommands::Lifetimes {
                endpoint,
                output_path,
                cache_file,
            } => {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        analyse_lifetimes(&endpoint, &output_path, cache_file).await;
                    });
            }
        },
        Commands::Stream(mut stream_args) => {
            if stream_args.num_jobs == 0 {
                stream_args.num_jobs = 1;
            }
            // create the Tokio runtime and run the extraction
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    run_stream_extraction(stream_args).await;
                });
        }
    }
}

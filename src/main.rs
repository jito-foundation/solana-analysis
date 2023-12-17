use anchor_lang::Discriminator;
use clap::Parser;
use jito_tip_distribution::state::ClaimStatus;
use log::info;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcProgramAccountsConfig;
use solana_rpc_client_api::filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use std::time::Duration;

#[derive(Parser)]
struct Args {
    /// RPC url to use
    #[arg(short, long)]
    rpc_url: String,
}

async fn print_claim_status_info(rpc_client: &RpcClient) {
    info!("calling getPA on tip programs to find the ClaimStatus accounts...");

    // this call takes awhile, make sure RpcClient has properly configured timeout
    let claim_status_accounts = rpc_client
        .get_program_accounts_with_config(
            &jito_tip_distribution::id(),
            RpcProgramAccountsConfig {
                filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new(
                    0,
                    MemcmpEncodedBytes::Bytes(ClaimStatus::discriminator().to_vec()),
                ))]),
                account_config: Default::default(),
                with_context: None,
            },
        )
        .await
        .unwrap();

    info!("claim status accounts: {:?}", claim_status_accounts.len());
    let claim_status_size = claim_status_accounts.first().unwrap().1.data.len();

    let min_rent = rpc_client
        .get_minimum_balance_for_rent_exemption(claim_status_size)
        .await
        .unwrap() as usize;

    let total_claim_status_rent = min_rent * claim_status_accounts.len();
    let total_rent = total_claim_status_rent as f64 / LAMPORTS_PER_SOL as f64;
    info!("using {} SOL on ClaimStatus rent", total_rent);
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let rpc_client = RpcClient::new_with_timeout(args.rpc_url, Duration::from_secs(300));

    print_claim_status_info(&rpc_client).await;
}

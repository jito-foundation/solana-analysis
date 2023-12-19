use anchor_lang::Discriminator;
use clap::Parser;
use jito_tip_distribution::state::ClaimStatus;
use log::info;
use solana_account_decoder::UiAccountEncoding;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_rpc_client_api::filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::stake;
use solana_sdk::stake::state::StakeState;
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
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..RpcAccountInfoConfig::default()
                },
                with_context: None,
            },
        )
        .await
        .unwrap();

    // all ClaimStatus accounts should be the same size
    let claim_status_size = claim_status_accounts.first().unwrap().1.data.len();
    let min_rent = rpc_client
        .get_minimum_balance_for_rent_exemption(claim_status_size)
        .await
        .unwrap() as usize;

    let total_claim_status_rent = min_rent * claim_status_accounts.len();
    let total_rent = total_claim_status_rent as f64 / LAMPORTS_PER_SOL as f64;
    info!(
        "using {} SOL for rent on {} ClaimStatus accounts. Each account is {} bytes and the minimum rent amount is {}",
        total_rent,
        claim_status_accounts.len(),
        claim_status_size,
        min_rent
    );
}

async fn print_stake_account_info(rpc_client: &RpcClient) {
    // this call takes awhile, make sure RpcClient has properly configured timeout
    let stake_accounts = rpc_client
        .get_program_accounts_with_config(
            &stake::program::id(),
            RpcProgramAccountsConfig {
                filters: Some(vec![RpcFilterType::DataSize(
                    stake::state::StakeState::size_of() as u64,
                )]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..RpcAccountInfoConfig::default()
                },
                with_context: None,
            },
        )
        .await
        .unwrap();
    info!(
        "There are {} stake accounts on mainnet-beta",
        stake_accounts.len()
    );

    let min_rent_amount = rpc_client
        .get_minimum_balance_for_rent_exemption(StakeState::size_of())
        .await
        .unwrap();
    info!(
        "The minimum rent amount for stake state is {} lamports",
        min_rent_amount
    );

    // let excess_staked_accounts: Vec<_> = stake_accounts
    //     .iter()
    //     .filter_map(|(pubkey, stake_account)| {
    //         let state = StakeState::deserialize(&mut stake_account.data.as_slice()).ok()?;
    //         let delegation = state.delegation()?;
    //         let excess = stake_account.lamports - delegation.stake - min_rent_amount;
    //         info!(
    //             "{}: delegation stake: {:?}, lamports: {}, rent: {}, excess: {}",
    //             pubkey, delegation.stake, stake_account.lamports, min_rent_amount, excess
    //         );
    //
    //         Some((pubkey, excess))
    //     })
    //     .collect();
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let rpc_client = RpcClient::new_with_timeout(args.rpc_url, Duration::from_secs(600));

    print_transaction_fees().await;

    print_claim_status_info(&rpc_client).await;

    print_stake_account_info(&rpc_client).await;
}

async fn print_transaction_fees() {
    // let pubkey = Pubkey::from_str("GZctHpWXmsZC1YHACTGGcHhYxjdRqQvTpYkb9LMvxDib").unwrap();
    // let ledger_storage = LedgerStorage::new(true, None, None).await.unwrap();

    // go through GZctHpWXmsZC1YHACTGGcHhYxjdRqQvTpYkb9LMvxDib, summing up transaction fees paid
}

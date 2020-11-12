
extern crate web3;
extern crate clap;
extern crate rand;

use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::str::FromStr;

use clap::{App, Arg};

use web3::futures::{Future, Stream};
use web3::types::{BlockId, BlockNumber, TransactionId, CallRequest, Address, U256, FilterBuilder, H256};
use web3::contract::Contract;

mod erc20;


/// Timer which measures exp. moving average between start() and stop()
struct Timer {
    last: Option<f64>,
    num: u64,
    total: f64,
    name: String,
}

impl Timer {

    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            last: None,
            total: 0_f64,
            num: 0_u64,
        }
    }

    pub fn update(&mut self, duration: f64) -> () {

        self.num += 1;
        self.total += duration;
        self.last = Some(duration);

    }

}

fn main() {

    let matches = App::new("Test Web3 Performance")
        .arg(
            Arg::with_name("server")
                .long("server")
                .short("s")
                .takes_value(true)
                .help("WS RPC server address, defaults to as ws://127.0.0.1:8546")
        )
        .get_matches();

    let web3_addr = matches.value_of("server").unwrap_or("ws://127.0.0.1:8546");


    // ERC20 transfer
    let transfer_topic =  H256::from_str("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").unwrap();

    // UniswapV2 Swap
    let uniswap_topic = H256::from_str("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822").unwrap();

    let mut timers = Vec::new();
    let mut handles = Vec::new();

    let starting_time = Instant::now();

    // threads for querying state sequentially
    for n in 0..2 {
        let web3_addr = web3_addr.to_string();
        let name = format!("Batch_{}", n);
        let timer = Arc::new(RwLock::new(Timer::new(&name)));
        timers.push(timer.clone());
        handles.push(std::thread::spawn(move || run_query_state_on_new_block(&name, web3_addr, timer, false)));
    }

    // threads for querying state in batch
    for n in 0..2 {
        let web3_addr = web3_addr.to_string();
        let name = format!("Seq_{}", n);
        let timer = Arc::new(RwLock::new(Timer::new(&name)));
        timers.push(timer.clone());
        handles.push(std::thread::spawn(move || run_query_state_on_new_block(&name, web3_addr, timer, true)));
    }

    // thread for getting logs on every new block
    {
        let web3_addr = web3_addr.to_string();
        let name = "TransTest";
        let timer = Arc::new(RwLock::new(Timer::new(&name)));
        timers.push(timer.clone());
        handles.push(std::thread::spawn(move || run_get_logs_on_new_block(&name, web3_addr, timer, transfer_topic)));
    }

    // thread for getting logs on every new block
    {
        let web3_addr = web3_addr.to_string();
        let name = "UswTest";
        let timer = Arc::new(RwLock::new(Timer::new(&name)));
        timers.push(timer.clone());
        handles.push(std::thread::spawn(move || run_get_logs_on_new_block(&name, web3_addr, timer, uniswap_topic)));
    }

    // sub to pending transacitona and some tracing threa
    {
        let web3_addr = web3_addr.to_string();
        let name = "PendTrace";
        let timer = Arc::new(RwLock::new(Timer::new(&name)));
        timers.push(timer.clone());
        handles.push(std::thread::spawn(move || run_pending_tracing(web3_addr, timer)));
    }

    // every few seconds report pending transcation results
    {
        handles.push(std::thread::spawn(move || report_timers(starting_time, timers)));
    }

    handles.into_iter().for_each(|h| h.join().unwrap());
}

fn create_web3_ws(addr: &str) -> (web3::Web3<web3::transports::WebSocket>, web3::transports::EventLoopHandle) {
    let (event_loop, transport) = web3::transports::WebSocket::new(addr)
        .expect("Error creating web3");
    let web3 = web3::Web3::new(transport);
    (web3, event_loop)
}

fn report_timers(starting_time: Instant, timers: Vec<Arc<RwLock<Timer>>>) {
    loop {

        std::thread::sleep(std::time::Duration::from_secs(10));

        let e = starting_time.elapsed().as_secs_f64() * 100_f64;

        let mut total_sum = 0_f64;

        for t in timers.iter() {
            let t = t.read().unwrap();
            let avg = t.total / t.num as f64 * 1000_f64;
            let rel = t.total / e * 100_f64;
            total_sum += t.total;
            println!("{:<10}: avg: {:>7.1} ms, rel: {:>7.4}%, iter: {}", t.name, avg, rel, t.num);
        }
        println!("TOTAL rel: {:>7.4}%", total_sum / e * 100_f64);

    }
}

fn run_pending_tracing(web3_addr: String, timer: Arc<RwLock<Timer>>) {

    let (web3, _eloop) = create_web3_ws(&web3_addr);

    web3
        .eth_subscribe()
        .subscribe_new_pending_transactions()
        .wait() 
        .expect("Could not get pending sub.")
        .and_then(|tx_hash| {
            let now = Instant::now();
            web3
                .eth()
                .transaction(TransactionId::Hash(tx_hash))
                .and_then(move |tx| Ok((now, tx)))
        })
        .for_each(|(now, tx)| {

            if let Some(tx) = tx {
                if let Some(to) = tx.to {
                    let req = CallRequest {
                        from: Some(tx.from),
                        to,
                        gas: Some(tx.gas),
                        gas_price: Some(tx.gas_price),
                        value: Some(tx.value),
                        data: Some(tx.input.clone())
                    };
                    let _ = web3.trace()
                        .call(
                            req,
                            vec![web3::types::TraceType::StateDiff],
                            Some(BlockNumber::Latest))
                        .wait()
                        .expect("trace error");

                    let dur = now.elapsed().as_secs_f64();
                    let mut timer = timer.write().unwrap();
                    timer.update(dur);
                    if timer.num % 1000 == 0 {
                        println!(
                            "...PendTrace: avg: {:>7.1} ms, num iter: {}", 
                            timer.total / timer.num as f64 * 1000_f64, 
                            timer.num
                        );
                    }
                }
            }

            Ok(())
        })
        .wait()
        .unwrap();

}

fn run_query_state_on_new_block(name: &str, web3_addr: String, timer: Arc<RwLock<Timer>>, batch: bool) {

    let (web3, _eloop) = create_web3_ws(&web3_addr);

    let batch_web3 = {
        let batch_transport = web3::transports::Batch::new(web3.transport().clone());
        web3::Web3::new(batch_transport)
    };

    // for testing using a few common tokens
    let addresses = vec![
        Address::from_str("6b175474e89094c44da98b954eedeac495271d0f").unwrap(), // DAI
        Address::from_str("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap(), // USDC
        Address::from_str("dac17f958d2ee523a2206206994597c13d831ec7").unwrap(), // USDT
        Address::from_str("2260fac5e5542a773aa44fbcfedf7c193bc2c599").unwrap(), // WBTC
        Address::from_str("514910771af9ca656af840dff83e8264ecf986ca").unwrap(), // LINK
        Address::from_str("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2").unwrap(), // WETH
        Address::from_str("1f9840a85d5af5bf1d1762f925bdaddc4201f984").unwrap(), // UNI
        Address::from_str("7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9").unwrap(), // AAVE
        Address::from_str("c00e94cb662c3520282e6f5717214004a7f26888").unwrap(), // COMP
        Address::from_str("ba100000625a3754423978a60c9317c58a424e3d").unwrap(), // BAL
    ];

    // for testing using a few top DAI and WETH holders as reported by etherscan
    let holders = vec![
        Address::from_str("a478c2975ab1ea89e8196811f51a7b7ade33eb11").unwrap(),
        Address::from_str("bebc44782c7db0a1a60cb6fe97d0b483032ff1c7").unwrap(),
        Address::from_str("b0fa2beee3cf36a7ac7e99b885b48538ab364853").unwrap(),
        Address::from_str("47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503").unwrap(),
        Address::from_str("c3d03e4f041fd4cd388c549ee2a29a9e5075882f").unwrap(),
        Address::from_str("7d6149ad9a573a6e2ca6ebf7d4897c1b766841b4").unwrap(),
        Address::from_str("acd43e627e64355f1861cec6d3a6688b31a6f952").unwrap(),
        Address::from_str("3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3").unwrap(),
        Address::from_str("a5407eae9ba41422680e2e00537571bcc53efbfd").unwrap(),
        Address::from_str("231b7589426ffe1b75405526fc32ac09d44364c4").unwrap(),
        Address::from_str("6dcb8492b5de636fd9e0a32413514647d00ef8d0").unwrap(),
        Address::from_str("250e76987d838a75310c34bf422ea9f1ac4cc906").unwrap(),
        Address::from_str("16de59092dae5ccf4a1e6439d611fd0653f0bd01").unwrap(),
        Address::from_str("223034edbe95823c1160c16f26e3000315171ca9").unwrap(),
        Address::from_str("bb2b8038a1640196fbe3e38816f3e67cba72d940").unwrap(),
        Address::from_str("2f0b23f53734252bda2277357e97e1517d6b042a").unwrap(),
        Address::from_str("b4e16d0168e52d35cacd2c6185b44281ec28c9dc").unwrap(),
        Address::from_str("0d4a11d5eeaac28ec3f61d100daf4d40471f1852").unwrap(),
        Address::from_str("3605ec11ba7bd208501cbb24cd890bc58d2dba56").unwrap(),
        Address::from_str("1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e").unwrap(),
        Address::from_str("1eff8af5d577060ba4ac8a29a13525bb0ee2a3d5").unwrap(),
        Address::from_str("d3d2e2692501a5c9ca623199d38826e513033a17").unwrap(),
    ];

    let contracts: Vec<_> = addresses
        .iter()
        .map(|a| create_erc20_contract(&web3, *a))
        .collect();

    let batch_contracts: Vec<_> = addresses
        .iter()
        .map(|a| create_erc20_contract(&batch_web3, *a))
        .collect();


    web3
        .eth_subscribe()
        .subscribe_new_heads()
        .wait()
        .expect("error subscribe new heads")
        .for_each(|head| {

            let now = Instant::now();

            let block_id;
            let block_number;

            if let Some(n) = head.number {
                block_id = Some(BlockId::Number(BlockNumber::Number(n)));
                block_number = n.as_u64();
            } else {
                return Ok(())
            };

            // NOTE: randomize queries 50:50 chance it gets executed
                        
            if batch {

                for batch_contract in batch_contracts.iter() {
                    let _ = batch_contract
                        .query::<U256, _, _, _>(
                            "totalSupply", (), None, web3::contract::Options::default(), block_id
                        );
                    for holder in holders.iter() {
                        if rand::random() {
                            let _ = batch_contract
                                .query::<U256, _, _, _>(
                                    "balanceOf", *holder, None, web3::contract::Options::default(), block_id
                                );
                        }
                    }
                }

            } else {

                for contract in contracts.iter() {
                    let _supply = contract
                        .query::<U256, _, _, _>(
                            "totalSupply", (), None, web3::contract::Options::default(), block_id
                        )
                        .wait()
                        .unwrap();
                    for holder in holders.iter() {
                        if rand::random() {
                            let _balance = contract
                                .query::<U256, _, _, _>(
                                    "balanceOf", *holder, None, web3::contract::Options::default(), block_id
                                )
                                .wait()
                                .unwrap();
                        }
                    }
                }

            }

            if batch {
                let _results: Vec<_> = batch_web3
                    .transport()
                    .submit_batch()
                    .wait()
                    .expect("error submit_batch")
                    .into_iter()
                    .collect(); 
            }

            let dur = now.elapsed().as_secs_f64();
            let mut timer = timer.write().unwrap();
            timer.update(dur);
            
            println!(
                "...{:<10}: block {} processing, last: {:>7.1} ms, avg: {:>7.1} ms, num iter: {}", 
                name,
                block_number,
                timer.last.unwrap_or(0_f64), 
                timer.total / timer.num as f64 * 1000_f64,
                timer.num
            );

            Ok(())
        })
        .wait()
        .unwrap();

}

fn run_get_logs_on_new_block(name: &str, web3_addr: String, timer: Arc<RwLock<Timer>>, topic: H256) {

    let (web3, _eloop) = create_web3_ws(&web3_addr);

    web3
        .eth_subscribe()
        .subscribe_new_heads()
        .wait()
        .expect("error subscribe new heads")
        .for_each(|head| {

            let now = Instant::now();

            let block_number;

            if let Some(n) = head.number {
                block_number = n.as_u64();
            } else {
                return Ok(())
            };

            let from_block_number = block_number - 20;

            let filter = FilterBuilder::default()
                .from_block(BlockNumber::Number(from_block_number.into()))
                .to_block(BlockNumber::Number(block_number.into()))
                .topics(
                    Some(vec![ topic ]), 
                    None,
                    None,
                    None)
                .build();

            let logs = web3
                .eth()
                .logs(filter)
                .wait()
                .expect("error getting logs");

            let dur = now.elapsed().as_secs_f64();
            let mut timer = timer.write().unwrap();
            timer.update(dur);

            println!(
                "...{:<10}: got {:>5} logs on block {}, last: {:>7.1} ms, avg: {:>7.1} ms, num iter: {}", 
                name,
                logs.len(),
                block_number,
                timer.last.unwrap_or(0_f64), 
                timer.total / timer.num as f64 * 1000_f64,
                timer.num
            );

            Ok(())
        })
        .wait()
        .unwrap();

}

pub fn create_erc20_contract<T: web3::Transport>(web3: &web3::Web3<T>, token_address: Address) -> Contract<T> {
    Contract::from_json(web3.eth(), token_address, erc20::ERC20_ABI.as_bytes()).expect("error creating contract")
}








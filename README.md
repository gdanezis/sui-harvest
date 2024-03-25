# sui-harvest : a utility to print the latest Sui Blockchain events

```
A simple event monitor and library to consume events from the Sui blockchain

Usage: harvest [OPTIONS]

Options:
  -c, --count <COUNT>                                Number of checkpoints to process [default: 10]
      --concurrent <CONCURRENT>                      Number of checkpoints to process [default: 5]
  -f, --follow                                       Whether to follow in real time
  -s, --suppress <SUPPRESS>                          Bottom percentage to suppress [default: 0.5]
      --full-node-url <FULL_NODE_URL>                URL of Sui full nodes [default: https://fullnode.mainnet.sui.io:443]
      --checkpoints-node-url <CHECKPOINTS_NODE_URL>  URL of Sui checkpoint nodes [default: https://checkpoints.mainnet.sui.io]
  -h, --help                                         Print help
  -V, --version
```

Sample output

```
$ cargo run -- --count 10
Sui mainnet version: 1.20.1
Get events from checkpoints 29699538 ... 29699548
33    cb4e1ee2a3d6323c70e7b06a8638de6736982cbdc08317d33e6f098747e2b438
          13 : margin_bank::BankBalanceUpdateV2
           8 : order::OrderFillV2
           8 : position::AccountPositionUpdateEventV2
           4 : isolated_trading::TradeExecutedV2
26    000000000000000000000000000000000000000000000000000000000000dee9
          10 : clob_v2::OrderCanceled<sui::SUI, coin::COIN>
           9 : clob_v2::OrderPlaced<sui::SUI, coin::COIN>
           2 : clob_v2::AllOrdersCanceled<sui::SUI, coin::COIN>
           1 : clob_v2::OrderCanceled<coin::COIN, coin::COIN>
           1 : clob_v2::AllOrdersCanceled<coin::COIN, coin::COIN>
           1 : clob_v2::OrderPlaced<coin::COIN, coin::COIN>
           1 : clob_v2::OrderPlaced<coin::COIN, coin::COIN>
           1 : clob_v2::OrderFilled<sui::SUI, coin::COIN>
13    1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb
           6 : pool::SwapEvent
           2 : pool::CollectFeeEvent
           1 : pool::RemoveLiquidityEvent
           1 : pool::CollectRewardEvent
           1 : pool::ClosePositionEvent
           1 : pool::OpenPositionEvent
           1 : pool::AddLiquidityEvent
10    5d8fbbf6f908a4af8c6d072669a462d53e03eb3c1d863bd0359dc818c69ea706
          10 : SupraSValueFeed::SCCProcessedEvent
4     d899cf7d2b5db716bd2cf55599fb0d5ee38a3061e7b6bb6eebf73fa5bc4c81ca
           1 : lending::DepositEvent
           1 : pool::PoolDeposit
           1 : lending::WithdrawEvent
           1 : pool::PoolWithdraw
2     91bfbc386a41afcfd9b2533058d7e915a1d3829089cc268ff4333d54d6339ca1
           2 : pool::SwapEvent
2     dc6160acd35ecf8d86a945525b53399723f76e971f35cc6f4f699a583f94303b
           2 : coin_flip_v2::NewGame<sui::SUI>
1     fadc2a695e4d92612a203f5bfc48e9f0a5be4d6bad9d08b1e8f06ce9be7a3f5a
           1 : events::DepositEvent
1     dc15721baa82ba64822d585a7349a1508f76d94ae80e899b06e48369c257750e
           1 : swap_cap::SwapCompletedEvent
1     efe170ec0be4d762196bedecd7a065816576198a6527c99282a2551aaa7da38c
           1 : events::SwapEvent
1     e66f07e2a8d9cf793da1e0bca98ff312b3ffba57228d97cf23a0613fddf31b65
           1 : incentive_v2::RewardsClaimed
1     26efee2b51c911237888e5dc6702868abca3c7ac12c53f76ef8eba0697695e3d
           1 : complete_transfer::TransferRedeemed
```
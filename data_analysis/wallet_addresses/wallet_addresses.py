#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 28 08:45:31 2023

@author: tono
"""
import json

# =============================================================================
# Exchange addresses
# =============================================================================

exchange_addresses = {
    'hx1729b35b690d51e9944b2e94075acff986ea0675': 'binance_cold_01',
    'hx99cc8eb746de5885f5e5992f084779fc0c2c135b': 'binance_cold_02',
    'hx9f0c84a113881f0617172df6fc61a8278eb540f5': 'binance_cold_03',
    'hxc4193cda4a75526bf50896ec242d6713bb6b02a3': 'binance_hot',
    'hxa527f96d8b988a31083167f368105fc0f2ab1143': 'binance.us',
    'hx307c01535bfd1fb86b3b309925ae6970680eb30d': 'velic_hot',
    'hxff1c8ebad1a3ce1ac192abe49013e75db49057f8': 'velic_stave',
    'hx14ea4bca6f205fecf677ac158296b7f352609871': 'latoken',
    'hx3881f2ba4e3265a11cf61dd68a571083c7c7e6a5': 'coinex',
    'hxd9fb974459fe46eb9d5a7c438f17ae6e75c0f2d1': 'huobi',
    'hx68646780e14ee9097085f7280ab137c3633b4b5f': 'kraken_hot',
    'hxbf90314546bbc3ed980454c9e2a9766160389302': 'upbit_hot_old',
    'hx562dc1e2c7897432c298115bc7fbcc3b9d5df294': 'upbit_cold',
    'hxb7f3d4bb2eb521f3c68f85bbc087d1e56a816fd6': 'crypto.com_hot_01',
    'hx294c5d0699615fc8d92abfe464a2601612d11bf7': 'crypto.com_hot_02',
    'hxddec6fb21f9618b537e930eaefd7eda5682d9dc8': 'crypto.com_cold_01',
    'hx6332c8a8ce376a5fc7f976d1bc4805a5d8bf1310': 'upbit_hot_01',
    'hxfdb57e23c32f9273639d6dda45068d85ee43fe08': 'upbit_hot_02',
    'hx4a01996877ac535a63e0107c926fb60f1d33c532': 'upbit_hot_03',
    'hx8d28bc4d785d331eb4e577135701eb388e9a469d': 'upbit_hot_04',
    'hxf2b4e7eab4f14f49e5dce378c2a0389c379ac628': 'upbit_hot_05',
    'hx6eb81220f547087b82e5a3de175a5dc0d854a3cd': 'bithumb_01',
    'hx0cdf40498ef03e6a48329836c604aa4cea48c454': 'bithumb_02',
    'hx6d14b2b77a9e73c5d5804d43c7e3c3416648ae3d': 'bithumb_03',
    'hx85532472e789802a943bd34a8aeb86668bc23265': 'unkEx_c_01',
    'hx94a7cd360a40cbf39e92ac91195c2ee3c81940a6': 'unkEx_c_02',
    'hxe5327aade005b19cb18bc993513c5cfcacd159e9': 'XGO',
    'hx23cb1d823ef96ac22ae30c986a78bdbf3da976df': 'bitvavo_cold_01',
    'hxd7a34c15c2345d9f0891545350181c7b162d9e08': 'bitvavo_cold_02',
    'hxa390d24fdcba68515b492ffc553192802706a121': 'bitvavo_hot',                
    'hx6b91c8dea3114de74ecfa85908b875778c2b599c': 'bybit',
    'hxbb984f278abd66ae2155edbd91f8c87ce98411c8': 'kucoin',
    'hxc8377a960d4eb484a3b8a733012995583dda0813': 'easy_crypto',
    }

json_exchange_addresses = json.dumps(exchange_addresses, indent=4)
with open("./ICONProject/data_analysis/wallet_addresses/exchange_addresses.json", "w") as outfile:
    outfile.write(json_exchange_addresses)

# =============================================================================
# Other addresses
# =============================================================================

other_addresses = {
    'hx02dd8846baddc171302fb88b896f79899c926a5a':'ICON_Vote_Monitor',
    'hx44c0d5fab0c81fe01a052f5ffb83fd152e505202':'facilitator_1',
    
    'hx8f0c9200f58c995fb28029d83adcf4521ff5cb2f':'LDX Distro',
    
    'hxbdd5ba518b70408acd023a18e4d6b438c7f11655':'Somesing Exchange',
    
    'hx037c73025819e490e9a01a7e954f9b46d89b0245':'MyID_related_1',
    'hx92b7608c53825241069a280982c4d92e1b228c84':'MyID_related_2',
    
    'hx522bff55a62e0c75a1b51855b0802cfec6a92e84':'3-min_tx_bot_out',
    'hx11de4e28be4845de3ea392fd8d758655bf766ca7':'3-min_tx_bot_in',
    
    # 'hx7a649b6b2d431849fd8e3db2d4ed371378eacf78':'icf_related1',
    # 'hx63862927a9c1389e277cd20a6168e51bd50af13e':'icf_related2',
    
    # binance sweepers
    'hx8a50805989ceddee4341016722290f13e471281e':'binance\nsweeper_01',
    'hx58b2592941f61f97c7a8bed9f84c543f12099239':'binance\nsweeper_02',
    'hx49c5c7eead084999342dd6b0656bc98fa103b185':'binance\nsweeper_03',
    'hx56ef2fa4ebd736c5565967197194da14d3af88ca':'binance\nsweeper_04',
    'hxe295a8dc5d6c29109bc402e59394d94cf600562e':'binance\nsweeper_05',
    'hxa479f2cb6c201f7a63031076601bbb75ddf78670':'binance\nsweeper_06',
    'hx538de7e0fc0d312aa82549aa9e4daecc7fabcce9':'binance\nsweeper_07',
    'hxc20e598dbc78c2bfe149d3deddabe77a72412c92':'binance\nsweeper_08',
    'hx5fd21034a8b54679d636f3bbff328df888f0fe28':'binance\nsweeper_09',
    'hxa94e9aba8830f7ee2bace391afd464417284c430':'binance\nsweeper_10',
    'hxa3453ab17ec5444754cdc5d928be8a49ecf65b22':'binance\nsweeper_11',
    'hx8c0a2f8ca8df29f9ce172a63bf5fd8106c610f42':'binance\nsweeper_12',
    'hx663ff2514ece0de8c3ecd76f003a9682fdc1fb00':'binance\nsweeper_13',
    'hx0232d71f68846848b00b4008771be7b0527fbb39':'binance\nsweeper_14',
    'hx561485c5ee93cf521332b520bb5d10d9389c8bab':'binance\nsweeper_15',
    'hx0b75cf8b1cdb81d514e64cacd82ed14674513e6b':'binance\nsweeper_16',
    'hx02ebb44247de11ab80ace2e3c25ebfbcffe4fa68':'binance\nsweeper_17',
    'hxc000d92a7d9d316c6acf11a23a4a20030d414ef2':'binance\nsweeper_18',
    'hx7135ddaeaf43d87ea73cbdd22ba202b13a2caf6a':'binance\nsweeper_19',
    'hxb2d0da403832f9f94617f5037808fe655434e5b7':'binance\nsweeper_20',
    'hx387f3016ee2e5fb95f2feb5ba36b0578d5a4b8cf':'binance\nsweeper_21',
    'hx69221e58dfa8e3688fa8e2ad368d78bfa0fad104':'binance\nsweeper_22',
    
    'hx1a5f0ce1d0d49054379a554f644f39a66a979b04':'circle_arb_1_related',
    'hx5b802623eb53c6a90df9f29e5808596f3c2bf63e':'circle_arb_2_related'
    }

json_other_addresses = json.dumps(other_addresses, indent=4)
with open("./ICONProject/data_analysis/wallet_addresses/other_addresses.json", "w") as outfile:
    outfile.write(json_other_addresses)
# =============================================================================
# Contract addresses
# =============================================================================

contract_addresses = {
    'cxb0b6f777fba13d62961ad8ce11be7ef6c4b2bcc6': 'ICONbet\n DAOdice(new),',
    'cx1c06cf597921e343dfca2883f699265fbec4d578': 'ICONbet\n Lottery(new),',
    'cx5d6e1482434085f30660efe30573304d629270e5': 'ICONbet\n Baccarat',
    'cx38fd2687b202caf4bd1bda55223578f39dbb6561': 'ICONbet\n Mini Roulette(new),',
    'cx6cdbc291c73faf79366d35b1491b89217fdc6638': 'ICONbet\n War',
    'cx8f9683da09e251cc2b67e4b479e016550f154bd6': 'ICONbet\n Hi - Lo',
    'cxd47f7d943ad76a0403210501dab03d4daf1f6864': 'ICONbet\n Blackjack',
    'cx299d88908ab371d586c8dfe0ed42899a899e6e5b': 'ICONbet\n Levels',
    'cxca5df10ab4f46df979aa2d38b370be85076e6117': 'ICONbet\n Colors',
    'cx03c76787861eec166b25e744e52a82af963670eb': 'ICONbet\n Plinko',
    'cx26b5b9990e78c6afe4f9d30776a43b1c19f7d85a': 'ICONbet\n Sic Bo',
    'cx9fda786d3e7965ed9dc01321c85026653d6a5ff4': 'ICONbet\n Jungle Jackpot',
    'cx3b9955d507ace8ac27080ed64948e89783a62ab1': 'ICONbet\n Reward',
    'cx1b97c1abfd001d5cd0b5a3f93f22cccfea77e34e': 'ICONbet\n Game Contract',
    'cxc6bb033f9d0b2d921887040b0674e7ceec1b769c': 'Lossless Lottery',
    'cx14002628a4599380f391f708843044bc93dce27d': 'iAM Div',
    'cx75e584ffe40cf361b3daa00fa6593198d47505d5': 'TAP Div',
    'cxff66ea114d20f6518e89f1269b4a31d3620b9331': 'PGT Distro',
    'cx953260a551584681e1f0492dce29e07d323ed5a6': 'ICONPOOL',
    'cx087b4164a87fdfb7b714f3bafe9dfb050fd6b132': 'Relay_1',
    'cx2ccc0c98ab5c2709cfc2c1512345baa99ea4106a': 'Relay_2',
    'cx735704cf28098ea43cae2a8325c35a3e7f2a5d1c': 'Relay_3',
    'cx9e3cadcc1a4be3323ea23371b84575abb32703ae': 'MyID_1',
    'cx694e8c9f1a05c8c3719f30d46b97697960e4289e': 'MyID_2',
    'cxba62bb61baf8dd8b6a04633fe33806547785a00c': 'MyID_3',
    'cx636caea5cf5a336d33985ae12ae1839821a175a4': 'SEED_1',
    'cx2e138bde7e4cb3706c7ac3c881fbd165dce49828': 'SEED_2',
    'cx3c08892673803db95c617fb9803c3653f4dcd4ac': 'SEED_3',
    'cx32b06547643fead9048ea912ba4c03419ee97052': 'FutureICX',
    'cx9c4698411c6d9a780f605685153431dcda04609f': 'Auction',
    'cx334beb9a6cde3bf1df045869447488e0de31df4c': 'circle_arb_1',
    'cx9df59cf2c7dc7ae2dbdec4a10b295212595f2378': 'circle_arb_2',
    'cx05874afb081257373c89491d6dc63faefb428bb9': 'circle_arb_3',
    'cxcc711062b732ed14954008da8a5b5193b4d48618': 'peek_1',
    'cxa89982990826b66d86ef31275e93275dfddabfde': 'peek_2',
    'cxcaef4255ec5cb784594655fa5ff62ce09a4f8dfa': 'peek_3',
    'cxfb832c213401d824b9725b5cca8d75b734fd830b': 'rev_share',
    'cxbb2871f468a3008f80b08fdde5b8b951583acf06': 'Stably_USD',
    'cx7d8caa66cbe1a96876e0bc2bda4fc60e5f9781e6': 'ICX_Card',
    'cx82c8c091b41413423579445032281bca5ac14fc0': 'Craft',
    'cx8683d50b9f53275081e13b64fba9d6a56b7c575d': 'gangstabet_trade',
    'cx6139a27c15f1653471ffba0b4b88dc15de7e3267': 'gangstabet_token',
    
    'cx66d4d90f5f113eba575bf793570135f9b10cece1': 'balanced_loans',
    'cx43e2eec79eb76293c298f2b17aec06097be606e0': 'balanced_staking',
    'cx203d9cd2a669be67177e997b8948ce2c35caffae': 'balanced_dividends',
    'cxf58b9a1898998a31be7f1d99276204a3333ac9b3': 'balanced_reserve',
    'cx835b300dcfe01f0bdb794e134a0c5628384f4367': 'balanced_daofund',
    'cx10d59e8103ab44635190bd4139dbfd682fa2d07e': 'balanced_rewards',
    'cxa0af3165c08318e988cb30993b3048335b94af6c': 'balanced_dex',
    'cx40d59439571299bca40362db2a7d8cae5b0b30b0': 'balanced_rebalancing',
    'cx44250a12074799e26fdeee75648ae47e2cc84219': 'balanced_governance',
    'cxe647e0af68a4661566f5e9861ad4ac854de808a2': 'balanced_oracle',
    'cx2609b924e33ef00b648a409245c7ea394c467824': 'balanced_sicx',
    'cx88fd7df7ddff82f7cc735c871dc519838cb235bb': 'bnUSD',
    'cxf61cd5a45dc9f91c15aa65831a30a90d59a09619': 'balanced_baln',
    'cxcfe9d1f83fa871e903008471cca786662437e58d': 'balanced_bwt',
    'cx13f08df7106ae462c8358066e6d47bb68d995b6d': 'balanced_dividends_old',
    'cxaf244cf3c7164fe6f996f398a9d99c4d4a85cf15': 'balanced_airdrip',
    'cx624af53e8954abed2acf18e6f8c9f35eae918244': 'balanced_retirebnUSD_1',
    'cxd4d8444d9ad73d80b5a1691e51dc4a4108d09473': 'balanced_retirebnUSD_2',
    
    'cx1a29259a59f463a67bb2ef84398b30ca56b5830a': 'omm_token',
    'cxcb455f26a2c01c686fa7f30e1e3661642dd53c0d': 'omm_lending',
    
    'cxaa99a164586883eed0322d62a31946dfa9491fa6': 'optimus_rewards',
    'cx7b1843b7ef5368a080ccf59c0a9d1bdec474f9f6': 'optimus_dividends',
    'cx5faae53c4dbd1fbe4a2eb4aab6565030f10da5c6': 'optimus_fee_handler',
    'cx9f734408d7434604bb9984fa5898a792670ea945': 'optimus_multisiglock_time_wallet',
    
    'cx6c8897b59a8e4a3f14865d74c1cc7a80fe82a48c': 'Monkeys',
    'cx43fa2fa3cdc8c5d48abd612eada8169d3d9b5a73': 'Home Ground Pay',
    'cxe7c05b43b3832c04735e7f109409ebcb9c19e664': 'iAM ',
    
    'cx0000000000000000000000000000000000000000': 'governance',
    'cx67e67f92e231627393ee0d184620162660c06a1c': 'BTP',
    'cx30f18d26f45d990112a4cd4825c0b79af73aac7c': 'Yetis',
    
    'cxc99c1dcc28c36a6383176c6d1aeea1e4d83e4a69': 'Wonderland',
    'cx997849d3920d338ed81800833fbb270c785e743d': 'Wonderland',
    'cx3ce3269704dd3a5e8e7d8012d4b383c4748ed7cc': 'Wonderland',
    
    'cxc2f3ea1c84cac895b3ab05681705d472002bfb1f': 'Mojos',
    
    'cxa82aa03dae9ca03e3537a8a1e2f045bcae86fd3f': 'Bridge',
    'cx0eb215b6303142e37c0c9123abd1377feb423f0e': 'Bridge',
    
    'cxe5c2c460364acc5f8c1d5ca925930043d8d9c9dd': 'GangstaBet Crown',
    }

json_contract_addresses = json.dumps(contract_addresses, indent=4)
with open("./ICONProject/data_analysis/wallet_addresses/contract_addresses.json", "w") as outfile:
    outfile.write(json_contract_addresses)


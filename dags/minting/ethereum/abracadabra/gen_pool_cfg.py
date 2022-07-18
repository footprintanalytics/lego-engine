from utils.constant import PROJECT_PATH
import web3
from web3 import Web3
import pandas

w3 = web3.main.Web3(
    Web3.HTTPProvider('https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161', request_kwargs={'timeout': 300}))

# {
#   address:'' 地址
#   collateral: '' 质押品
# }
pool_ls = []

# 找到所有池子地址
# 官网 https://docs.abracadabra.money/our-ecosystem/our-contracts
v2_flat_address_ls = [
    '0x7b7473a76D6ae86CE19f7352A1E89F6C9dc39020',
    '0x05500e2Ee779329698DF35760bEdcAAC046e7C27',
    '0x003d5A75d284824Af736df51933be522DE9Eed0f',
    '0x98a84EfF6e008c5ed0289655CcdCa899bcb6B99F',
    '0xEBfDe87310dc22404d918058FAa4D56DC4E93f0A',
    '0x0BCa8ebcB26502b013493Bf8fE53aA2B1ED401C1',
    '0x920D9BD936Da4eAFb5E25c6bDC9f6CB528953F9f',
    '0x4EAeD76C3A388f4a841E9c765560BBe7B3E4B3A0',
    '0x252dCf1B621Cc53bc22C256255d2bE5C8c32EaE4',
    '0x35a0Dd182E4bCa59d5931eae13D0A2332fA30321',
    '0xc1879bf24917ebE531FbAA20b0D05Da027B592ce',
    '0x9617b633EF905860D919b88E1d9d9a6191795341',
    '0xCfc571f3203756319c231d3Bc643Cee807E74636',
    '0x3410297D89dCDAf4072B805EFc1ef701Bb3dd9BF',
    '0x59e9082e068ddb27fc5ef1690f9a9f22b32e573f',
    '0x257101F20cB7243E2c7129773eD5dBBcef8B34E0',
    '0x6cbAFEE1FaB76cA5B5e144c43B3B50d42b7C8c8f',
    '0x551a7CfF4de931F32893c928bBc3D25bF1Fc5147',
    '0x6Ff9061bB8f97d948942cEF376d98b51fA38B91f',
    '0xbb02A884621FB8F5BFd263A67F58B65df5b090f3',
    '0xC319EEa1e792577C319723b5e60a15dA3857E7da',
    '0xFFbF4892822e0d552CFF317F65e1eE7b5D3d9aE6',
    '0x806e16ec797c69afa8590A55723CE4CC1b54050E',
    '0x6371EfE5CD6e3d2d7C477935b7669401143b7985',
    '0xbc36fde44a7fd8f545d459452ef9539d7a14dd63'
]
v2_flat_abi = '[{"inputs":[{"internalType":"contract IBentoBoxV1","name":"bentoBox_","type":"address"},{"internalType":"contract IERC20","name":"magicInternetMoney_","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint128","name":"accruedAmount","type":"uint128"}],"name":"LogAccrue","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"share","type":"uint256"}],"name":"LogAddCollateral","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"part","type":"uint256"}],"name":"LogBorrow","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"rate","type":"uint256"}],"name":"LogExchangeRate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"newFeeTo","type":"address"}],"name":"LogFeeTo","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"share","type":"uint256"}],"name":"LogRemoveCollateral","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"part","type":"uint256"}],"name":"LogRepay","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"feeTo","type":"address"},{"indexed":false,"internalType":"uint256","name":"feesEarnedFraction","type":"uint256"}],"name":"LogWithdrawFees","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"BORROW_OPENING_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"COLLATERIZATION_RATE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LIQUIDATION_MULTIPLIER","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"accrue","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"accrueInfo","outputs":[{"internalType":"uint64","name":"lastAccrued","type":"uint64"},{"internalType":"uint128","name":"feesEarned","type":"uint128"},{"internalType":"uint64","name":"INTEREST_PER_SECOND","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"skim","type":"bool"},{"internalType":"uint256","name":"share","type":"uint256"}],"name":"addCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"bentoBox","outputs":[{"internalType":"contract IBentoBoxV1","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"borrow","outputs":[{"internalType":"uint256","name":"part","type":"uint256"},{"internalType":"uint256","name":"share","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"claimOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"collateral","outputs":[{"internalType":"contract IERC20","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8[]","name":"actions","type":"uint8[]"},{"internalType":"uint256[]","name":"values","type":"uint256[]"},{"internalType":"bytes[]","name":"datas","type":"bytes[]"}],"name":"cook","outputs":[{"internalType":"uint256","name":"value1","type":"uint256"},{"internalType":"uint256","name":"value2","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"exchangeRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeTo","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes","name":"data","type":"bytes"}],"name":"init","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address[]","name":"users","type":"address[]"},{"internalType":"uint256[]","name":"maxBorrowParts","type":"uint256[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"contract ISwapper","name":"swapper","type":"address"}],"name":"liquidate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"magicInternetMoney","outputs":[{"internalType":"contract IERC20","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"masterContract","outputs":[{"internalType":"contract CauldronV2Flat","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"oracle","outputs":[{"internalType":"contract IOracle","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"oracleData","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pendingOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"reduceSupply","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"share","type":"uint256"}],"name":"removeCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"skim","type":"bool"},{"internalType":"uint256","name":"part","type":"uint256"}],"name":"repay","outputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newFeeTo","type":"address"}],"name":"setFeeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"totalBorrow","outputs":[{"internalType":"uint128","name":"elastic","type":"uint128"},{"internalType":"uint128","name":"base","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalCollateralShare","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"},{"internalType":"bool","name":"direct","type":"bool"},{"internalType":"bool","name":"renounce","type":"bool"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"updateExchangeRate","outputs":[{"internalType":"bool","name":"updated","type":"bool"},{"internalType":"uint256","name":"rate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"userBorrowPart","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"userCollateralShare","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"withdrawFees","outputs":[],"stateMutability":"nonpayable","type":"function"}]'
print(f'开始执行：{len(v2_flat_address_ls)}')
for index in range(len(v2_flat_address_ls)):
    contract_address = v2_flat_address_ls[index]
    print(contract_address)
    coin_contract = w3.eth.contract(address=Web3.toChecksumAddress(contract_address), abi=v2_flat_abi)
    collateral = coin_contract.functions.collateral().call()
    pool_ls.append({
        'pool_id': contract_address.lower(),
        'protocol_id': 908,
        'project': 'abracadabra',
        'chain': 'Ethereum',
        'business_type': 'minting',
        'deposit_contract': None,
        'withdraw_contract': None,
        'lp_token': None,
        'stake_token': collateral.lower(),
        'stake_underlying_token': None,
        'reward_token': None,
        'name': '',
        'description': None
    })

df = pandas.DataFrame.from_records(pool_ls)
df.to_csv(PROJECT_PATH + '/minting/ethereum/abracadabra/abracadabra_pool_stake_token.csv', index=False)

all_contract_address_str = '-'.join(v2_flat_address_ls)
print(f'共{len(pool_ls)}个池子')

import json
import datetime
import requests
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from config import SCAN_API_KEY, RPC_URL, MIN_BALANCE


w3 = Web3(Web3.HTTPProvider(RPC_URL))
w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)


def contract_analyzer(contract_address: str, start: str = None, end: str = None, action: str = None):
    pair_contract_address = Web3.to_checksum_address(contract_address)

    pair_abi = get_abi(contract_address)
    contract = w3.eth.contract(address=pair_contract_address, abi=pair_abi)

    if '"sqrtPriceX96"' in pair_abi and '"tick"' in pair_abi and '"liquidity"' in pair_abi:
        pool_version = True  # V3
    elif '"amount0In"' in pair_abi and '"amount1In"' in pair_abi and '"amount0Out"' in pair_abi and '"amount1Out"' in pair_abi:
        pool_version = False  # V2
    else: return

    if pool_version:
        swap_event_signature = '0x' + w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)").hex()
    else:
        swap_event_signature = '0x' + w3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()

    buy = {}
    sell = {}

    contract_creation_block = get_contract_creation_block(contract_address)
    start_block = datetime_to_block(start) if start else None
    from_block = start_block if start_block is not None and start_block > contract_creation_block else contract_creation_block

    last_creation_block = int(w3.eth.block_number)
    end_block = datetime_to_block(end) if end else None
    to_block = end_block if end_block is not None else last_creation_block

    if from_block > to_block:
        from_block, to_block = to_block, from_block

    for fb in range(from_block, to_block, 2_000):
        step = 2_000 if fb + 2_000 < to_block else to_block - fb

        logs = w3.eth.get_logs({
            'fromBlock': fb,
            'toBlock': fb + step,
            'address': pair_contract_address,
            'topics': [swap_event_signature]
        })

        for log in logs:
            sender = (w3.eth.get_transaction(log['transactionHash']))['from']

            swap_event = contract.events.Swap().process_log(log)

            if pool_version:  # V3
                if swap_event['args']['amount1'] > 0:
                    native_amount = float(f"{(swap_event['args']['amount1'] / 10 ** 18):.3f}")
                    buy[sender] = sell.get(sender, 0) + native_amount
                    print(f'{sender} купил на {native_amount} | Осталось: {to_block - log["blockNumber"]}')
                elif swap_event['args']['amount1'] < 0:
                    native_amount = -float(f"{(swap_event['args']['amount1'] / 10 ** 18):.3f}")
                    sell[sender] = sell.get(sender, 0) + native_amount
                    print(f'{sender} продал на {native_amount} | Осталось: {to_block - log["blockNumber"]}')

            else:  # V2
                if swap_event['args']['amount0Out']:
                    # tokens_amount = swap_event['args']['amount0Out'] / (10**decimals)
                    native_amount = float(f"{(swap_event['args']['amount1In'] / 10 ** 18):.3f}")
                    buy[sender] = buy.get(sender, 0) + native_amount
                    print(f'{sender} купил на {native_amount} | Осталось: {to_block - log["blockNumber"]}')
                elif swap_event['args']['amount1Out']:
                    # tokens_amount = swap_event['args']['amount0In'] / (10 ** decimals)
                    native_amount = float(f"{(swap_event['args']['amount1Out'] / 10 ** 18):.3f}")
                    sell[sender] = sell.get(sender, 0) + native_amount
                    print(f'{sender} продал на {native_amount} | Осталось: {to_block - log["blockNumber"]}')

    # профит = сумма продажи / сумма покупки
    if not action:
        profit = {}
        for sellers_address, total_sales in sell.items():
            try:
                profit[sellers_address] = total_sales / buy.get(sellers_address, total_sales)
            except ZeroDivisionError:
                pass

        profit = filter_balance(profit)
        profit = dict(sorted(profit.items(), key=lambda item: item[1], reverse=True))

        with open(f'{contract_address}.json', 'w') as f:
            json.dump(profit, f, indent=4)

    # вывод только тех, кто покупал
    elif action == 'buy':
        buy = filter_balance(buy)
        buy = dict(sorted(buy.items(), key=lambda item: item[1], reverse=True))

        with open(f'buy_{contract_address}.json', 'w') as f:
            json.dump(buy, f, indent=4)

    # вывод только тех, кто продавал
    elif action == 'sell':
        sell = filter_balance(sell)
        sell = dict(sorted(sell.items(), key=lambda item: item[1], reverse=True))

        with open(f'sell_{contract_address}.json', 'w') as f:
            json.dump(sell, f, indent=4)


def address_analyzer(address: str, start_time: None, end_time: None):
    pass


def datetime_to_block(date_time: str) -> int | None:
    date, time = date_time.split(' ')
    timestamp = int(
        datetime.datetime(int(date.split('.')[2]), int(date.split('.')[1]), int(date.split('.')[0]),
                          int(time.split(':')[0]), int(time.split(':')[1]), int(time.split(':')[2]),
                          tzinfo=datetime.timezone.utc).timestamp())

    url = (f'https://api.bscscan.com/api'
           f'?module=block'
           f'&action=getblocknobytime'
           f'&timestamp={timestamp}'
           f'&closest=before'
           f'&apikey={SCAN_API_KEY}')
    block = execute_query(url)

    if block:
        return int(block)
    return None


def get_abi(contract_address: str) -> int | None:
    url = (f'https://api.bscscan.com/api'
           f'?module=contract'
           f'&action=getabi'
           f'&address={contract_address}'
           f'&apikey={SCAN_API_KEY}')
    return execute_query(url)


def get_contract_creation_block(contract_address: str) -> int | None:
    url = (f'https://api.bscscan.com/api'
           f'?module=contract'
           f'&action=getcontractcreation'
           f'&contractaddresses={contract_address}'
           f'&apikey={SCAN_API_KEY}')
    data = execute_query(url)

    if data:
        tx_hash = data[0]['txHash']
    else:
        return None

    url = f'https://api.bscscan.com/api?module=proxy&action=eth_getTransactionByHash&txhash={tx_hash}&apikey={SCAN_API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return int(data['result']['blockNumber'], 16)
    else:
        print(f"HTTP Error: {response.status_code}")
        return None


def execute_query(url: str):
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if data['status'] == '1':
            return data['result']
        else:
            print(f"Error: {data['message']}")
            return None
    else:
        print(f"HTTP Error: {response.status_code}")
        return None


def get_native_token_price() -> float:
    response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=binancecoin&vs_currencies=usd').json()
    return response['binancecoin']['usd']


def filter_balance(d: dict) -> dict:
    native_token_price = get_native_token_price()
    d_copy = d.copy()

    if MIN_BALANCE > 0:
        for address in d.keys():
            balance = (w3.eth.get_balance(address) / 10 ** 18)
            if balance < MIN_BALANCE / native_token_price:
                del d_copy[address]

    return d_copy

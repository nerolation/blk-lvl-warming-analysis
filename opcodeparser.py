import os
import aiohttp
import asyncio
import pandas as pd
from typing import Tuple, List, Dict, Any, Optional
import requests
from web3 import Web3
import time
import re
from google.cloud import storage


def set_google_credentials(CONFIG, GOOGLE_CREDENTIALS):
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except:
        print(f"setting google credentials as global variable...")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CONFIG \
        + GOOGLE_CREDENTIALS or input("No Google API credendials file provided." 
        + "Please specify path now:\n")
        
set_google_credentials("../config/","google-creds.json")

INFURA_URL = ""

FOLDER = "blocks"

if not os.path.exists(FOLDER):
    os.mkdir(FOLDER)

w3 = Web3(Web3.HTTPProvider(INFURA_URL))

if not w3.is_connected():
    raise Exception("Failed to connect to the Ethereum node.")
    
BACKFILLING = True



async def get_opcodes_gas_costs_and_refunds(
    tx_hash: str,
    to: str,
    block_number: int,
    tx_id: int,
    session: Optional[aiohttp.ClientSession] = None
) -> Tuple[List, pd.DataFrame]:
    """
    Asynchronously retrieve and process transaction trace data.
    
    Args:
        tx_hash: Transaction hash
        to: Target address
        block_number: Block number
        tx_id: Transaction ID
        session: Optional aiohttp session for connection reuse
    
    Returns:
        Tuple containing opcode data and storage information
    """
    payload = {
        "method": "debug_traceTransaction",
        "params": [
            tx_hash,
            {
                "disableMemory": True,
                "disableStack": False,
                "disableStorage": True
            }
        ],
        "id": 1,
        "jsonrpc": "2.0"
    }

    should_close_session = session is None
    if should_close_session:
        session = aiohttp.ClientSession()

    try:
        async with session.post(INFURA_URL, json=payload) as response:
            response.raise_for_status()
            trace = await response.json()
            
            struct_logs = trace.get("result", {}).get("structLogs", [])
            
            storage_info = await asyncio.create_task(
                extract_storage_operations_async(trace, to)
            )
            
            if isinstance(storage_info, pd.DataFrame):
                storage_info["tx_hash"] = tx_hash
                storage_info["tx_index"] = tx_id

            log_data = [(log["pc"], log["op"], log["gasCost"], log.get("refund", 0))
                       for log in struct_logs]
            
            if not log_data:
                return [[], [], [], 0, []], pd.DataFrame()

            pcs, opcodes, gas_costs, step_refunds = zip(*log_data)
            
            total_refund = max(step_refunds, default=0)

            return [list(opcodes), list(gas_costs), list(step_refunds), total_refund, list(pcs)], storage_info

    except aiohttp.ClientError as e:
        print(f"Error processing transaction {tx_hash}: {str(e)}")
        return [[], [], [], 0, []], pd.DataFrame()
    
    finally:
        if should_close_session:
            await session.close()


    
# Fetch transactions from a specific block
def get_transactions_from_block(block_number):
    block = w3.eth.get_block(block_number, full_transactions=True)
    return block.transactions

async def extract_storage_operations_async(trace, to):
    struct_logs = trace.get("result", {}).get("structLogs", [])
    contract_stack = {}
    storage_operations = []
    
    for log in struct_logs:
        op = log['op']
        depth = log['depth']
        cost = log["gasCost"]
        #print(op)
        
        if op in {'CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL'}:
            contract_address = log['stack'][-2]
            contract_stack[depth + 1] = contract_address
            
            storage_operations.append({
                'contract': contract_stack.get(depth, None),
                'op': op,
                'address': contract_address,
                'cost': cost,
                #'to': to
            })
        
        elif op in {'SSTORE', 'SLOAD'}:
            current_contract = contract_stack.get(depth, None)
            storage_slot = log['stack'][-1]
            
            if op == 'SSTORE':
                sstore_value = log['stack'][-2]
                storage_operations.append({
                    'contract': current_contract,
                    'slot': storage_slot,
                    'value': sstore_value,
                    'cost': cost,
                    'op': op,
                })
            elif op == 'SLOAD':
                storage_operations.append({
                    'contract': current_contract,
                    'slot': storage_slot,
                    'value': None,
                    'cost': cost,
                    'op': op,
                    #'to': to
                })
        
        elif op in {'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH'}:
            address = log['stack'][-1]  # Address is the top of the stack
            storage_operations.append({
                'contract': contract_stack.get(depth, None),
                'op': op,
                'address': address,
                'cost': cost,
                #'to': to
            })
        
        elif op in {'RETURN', 'REVERT', 'STOP'}:
            # Remove the contract from the stack when returning from a call
            if depth in contract_stack:
                del contract_stack[depth]
    
    if len(storage_operations) > 0:
        storage_info = pd.DataFrame(storage_operations)            
        storage_info["contract"] = storage_info["contract"].fillna(to)
        return storage_info
    else:
        return []

async def process_multiple_transactions(tx_data_list: List[Tuple[str, str, int, int]]) -> List[Tuple[List, pd.DataFrame]]:
    """
    Process multiple transactions concurrently using a connection pool.
    
    Args:
        tx_data_list: List of tuples containing (tx_hash, to, block_number, tx_id)
    
    Returns:
        List of results for each transaction
    """
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_opcodes_gas_costs_and_refunds(tx_hash, to, block_num, tx_id, session)
            for tx_hash, to, block_num, tx_id in tx_data_list
        ]
        return await asyncio.gather(*tasks)

async def collect_block_transactions(block_number):
    data = []
    transactions = get_transactions_from_block(block_number)
    block_storage_info = []
    txs = []
    
    for tx in transactions:
        tx_hash = tx.hash.hex()
        to = tx.get("to", "0x0000000000000000000000000000000000000000") or "0x0000000000000000000000000000000000000000"
        tx_id = int(tx.transactionIndex)
        txs.append((tx_hash, to, block_number, tx_id))
    
    results = await process_multiple_transactions(txs)
    
    for (tx_hash, _, _, tx_id), (opcodes_gas_refunds, storage_info) in zip(txs, results):
        if isinstance(storage_info, pd.DataFrame) and not storage_info.empty:
            block_storage_info.append(storage_info)
        
        if opcodes_gas_refunds is not None:
            data.append({
                "block_number": block_number,
                "tx_hash": tx_hash,
                "tx_id": tx_id,
                "pcs": opcodes_gas_refunds[4],
                "opcodes": opcodes_gas_refunds[0],
                "gas_costs": opcodes_gas_refunds[1],
                "total_refund": opcodes_gas_refunds[3]
            })
    
    final_storage_info = pd.concat(block_storage_info, ignore_index=True) if block_storage_info else pd.DataFrame()
    
    return pd.DataFrame(data), final_storage_info
def save_storage_info(df, block_number):
    df.to_parquet(f"/mnt/hdd1/storage/storage_{block_number}.parquet", index=False, compression="snappy")
    print(f"stored /mnt/hdd1/storage/storage_{block_number}.parquet")

prune_count = 0
async def parse_block(block_number):
    global prune_count
    df, df2 = await collect_block_transactions(block_number)
    if isinstance(df2, pd.DataFrame):
        save_storage_info(df2, block_number)
    if "tx_hash" in df.columns:
        if len(df["tx_hash"].drop_duplicates()) > 0:
            all_empty_lists = df["opcodes"].apply(lambda x: len(x) == 0).all()
            if not all_empty_lists:
                df['total_refund'] = df['total_refund'].astype(str)
                df.to_parquet(f"/mnt/hdd1/opcodes/block_{block_number}_transactions.parquet", index=False, compression="snappy")
                print(f"stored /mnt/hdd1/opcodes/block_{block_number}_transactions.parquet")
                return True    
    prune_count += 1
    print(f"Nothing found for block {block_number}; It's likely that this block was pruned already.")
    if prune_count < 10:
        return True
    return False



def upload_parquet_to_gcs(
    df,
    block_number: int,
    bucket_name: str,
    temp_directory: str = '/tmp',
    chunk_size: int = 5 * 1024 * 1024
) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(f"opcodes/block_{block_number}_transactions.parquet")

    blob.chunk_size = chunk_size

    blob.upload_from_filename(f"/tmp/block_{block_number}_transactions.parquet")

    if os.path.exists(f"/tmp/block_{block_number}_transactions.parquet"):
        os.remove(f"/tmp/block_{block_number}_transactions.parquet")

    print(f"Uploaded '/tmp/block_{block_number}_transactions.parquet' to 'gs://{bucket_name}/opcodes/block_{block_number}_transactions.parquet'")


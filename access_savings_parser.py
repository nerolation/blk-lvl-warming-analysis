import os
import pandas as pd
import pyxatu
import numpy as np


xatu = pyxatu.PyXatu()
pd.set_option('display.max_colwidth', None)

FILENAME = "storage_epoch_roll.parquet"
FILENAME_SLOT = "slot_usage.parquet"
FROM_SCRATCH = True
SKIP = 0

storage_related_opcodes = [
    "SSTORE",
    "SLOAD"
]
other_opcodes = [
    "BALANCE",  # Account balance check
    "EXTCODESIZE",  # External account code size
    "EXTCODECOPY",  # External account code copy
    "EXTCODEHASH",  # External account code hash
    "CALL",     # External call
    "CALLCODE", # Deprecated: similar to CALL but retains context
    "DELEGATECALL", # External call with caller context
    "STATICCALL"    # External call without state modification
]

precompile_addresses = [
    "0x0000000000000000000000000000000000000001",  # ecrecover
    "0x0000000000000000000000000000000000000002",  # sha256
    "0x0000000000000000000000000000000000000003",  # ripemd160
    "0x0000000000000000000000000000000000000004",  # identity
    "0x0000000000000000000000000000000000000005",  # modexp
    "0x0000000000000000000000000000000000000006",  # ecadd (alt_bn128)
    "0x0000000000000000000000000000000000000007",  # ecmul (alt_bn128)
    "0x0000000000000000000000000000000000000008",  # pairing (alt_bn128)
    "0x0000000000000000000000000000000000000009",  # blake2f
    "0x000000000000000000000000000000000000000a",  # kzg verify
]

epoch_warm = None
rolling_block_warm = dict()
known = None
el_blocks = None
el_blocks_from = None
cl_blocks = None
block_number = None
epochs = None
slots = None
slot_usage = dict()
local_df = pd.DataFrame(columns=["block_number"])
def main():
    global local_df, epoch_warm, known, cl_blocks, el_blocks, el_blocks_from, block_number, epochs, slots, rolling_block_warm, slot_usage
    chunksize = 256
    latest_slot = xatu.get_slots(columns="max(slot) as slot", add_missed=False)["slot"][0]
    gas_used = xatu.get_slots(
        slot=[10_000_000, int(latest_slot)], 
        columns="execution_payload_block_number,execution_payload_gas_used",
        add_missed=False
    )
    gas_used.columns = ["block_number", "gas_used"]

    max_block = max([int(x.split("_")[1].split(".")[0]) for x in os.listdir("/mnt/hdd1/storage")])

    def get_el_blocks(min_block):
        el_blocks = xatu.execute_query(
        f"""
            SELECT DISTINCT transaction_hash, from_address, to_address FROM canonical_execution_transaction FINAL
            WHERE block_number >= {min_block}
            AND  block_number <= {min_block + chunksize + 100}
            AND meta_network_name = 'mainnet'
        """
        )
        el_blocks.columns = ["transaction_hash", "from_address", "to_address"]
        el_blocks_to = el_blocks[["transaction_hash", "to_address"]].set_index("transaction_hash").to_dict()["to_address"]
        el_blocks_from = el_blocks[["transaction_hash", "from_address"]].set_index("transaction_hash").to_dict()["from_address"]
        return el_blocks_to, el_blocks_from


    def get_max_el_block():
        max_block = xatu.execute_query(
        f"""
            SELECT max(block_number) FROM canonical_execution_transaction FINAL
            WHERE meta_network_name = 'mainnet'
            and block_number > 21600000
        """
        )
        max_block.columns = ["block_number"]
        max_block = int(max_block.block_number.iloc[0])
        return max_block

    def get_cl_blocks(min_block):
        cl_blocks = xatu.execute_query(
        f"""
            SELECT DISTINCT execution_payload_block_number, execution_payload_fee_recipient FROM canonical_beacon_block FINAL
            WHERE execution_payload_block_number >= {min_block}
            AND  execution_payload_block_number <= {min_block+chunksize+100}
            AND meta_network_name = 'mainnet'
        """
        )
        cl_blocks.columns = ["block_number", "execution_payload_fee_recipient"]
        cl_blocks = cl_blocks.set_index("block_number").to_dict()["execution_payload_fee_recipient"]    
        return cl_blocks

    def get_max_cl_block():
        max_block = xatu.execute_query(
        f"""
            SELECT max(execution_payload_block_number) FROM canonical_beacon_block FINAL
            WHERE meta_network_name = 'mainnet'
        """
        )
        max_block.columns = ["block_number"]
        max_block = int(max_block.block_number.iloc[0])
        return max_block

    def get_epochs(min_block):
        block_epoch = xatu.get_slots(slot=[10810000,int(latest_slot)], columns="execution_payload_block_number, epoch, slot", add_missed=False)
        block_epoch.columns = ["block_number", "epoch", "slot"]
        block_epoch = block_epoch[block_epoch["block_number"] >= min_block]
        _block_epoch = block_epoch[["block_number", "epoch"]].set_index("block_number").to_dict()["epoch"]
        block_epoch_slot = block_epoch[["block_number", "slot"]].set_index("block_number").to_dict()["slot"]
        return _block_epoch, block_epoch_slot

    maxcl = get_max_cl_block()
    maxel = get_max_el_block()

    diff = max_block - maxcl
    diff2 = max_block - maxel
    diff = max([diff, diff2])

    max_block = min([maxcl, max_block])
    max_block = min([maxel, max_block])
    print("diff: ", diff)

    el_blocks, cl_blocks, epochs, slots = dict(), dict(), dict(), dict()
    
    for i in range(1, 32):
        rolling_block_warm[i] = [set()]

    def get_files(local_df):
        global known
        known = set(local_df.block_number)
        l = len(os.listdir("/mnt/hdd1/storage")[32:])
        for ix, file in enumerate(sorted(os.listdir("/mnt/hdd1/storage"), key=lambda x: int(x.split("_")[1].split(".")[0]), reverse=False)[32:]):
            if ix < SKIP:
                continue
            block_number = int(file.split("_")[1].split(".")[0])
            if block_number in known:
                continue
            known.add(block_number)
            print("processing ", file)
            df = pd.read_parquet("/mnt/hdd1/storage/" + file)
            if "value" in df.columns:
                df.drop("value", inplace=True, axis=1)
            if "tx_index" in df.columns:
                df.drop("tx_index", inplace=True, axis=1)
            df["block_number"] = block_number
            yield df, ix, l

    def get_to_address(tx_hash, block_number):
        global el_blocks, el_blocks_from
        if tx_hash not in el_blocks.keys():
            el_blocks, el_blocks_from = get_el_blocks(block_number)
        return el_blocks[tx_hash]

    def get_from_address(tx_hash, block_number):
        global el_blocks, el_blocks_from
        if tx_hash not in el_blocks_from.keys():
            el_blocks, el_blocks_from = get_el_blocks(block_number)
        return el_blocks_from[tx_hash]

    def get_coinbase(block_number):
        global cl_blocks
        if block_number not in cl_blocks.keys():
            cl_blocks = get_cl_blocks(block_number)
            #print(cl_blocks)
        return cl_blocks[block_number]

    def get_epoch_of_block_number(block_number):
        global epochs, slots
        if block_number not in epochs.keys():
            epochs, slots = get_epochs(block_number)
            #print(cl_blocks)
        return epochs[block_number]

    def get_slot_of_block_number(block_number):
        global epochs, slots
        if block_number not in slots.keys():
            epochs, slots = get_epochs(block_number)
        return slots[block_number]

    def determine_warm_cold_access_fast(df, skip=999):
        global epoch_warm, rolling_block_warm, slot_usage
        print("determine_warm_cold_access...", end="\r")

        n_rows = len(df)

        tx_warm_col = np.zeros(n_rows, dtype=np.int8)
        block_warm_col = np.zeros(n_rows, dtype=np.int8)
        epoch_warm_col = np.zeros(n_rows, dtype=np.int8)
        block_warm_roll_cols = {k: np.zeros(n_rows, dtype=np.int8) for k in range(1, 32)}

        tx_warm = set(precompile_addresses)
        block_warm = set(precompile_addresses)

        try:
            
            block_number = int(df.block_number.iloc[0])
        except:
            block_warm_copy = set()
            for k in range(1, 32):
                if k < 31-(skip+1):
                    continue          
                rolling_block_warm[k].append(block_warm_copy)
                if len(rolling_block_warm[k]) >= k + 1:
                    rolling_block_warm[k] = rolling_block_warm[k][-k:]
                print(f"rolling blocks check: {k}=={len(rolling_block_warm[k])}")

            print("determine_warm_cold_access done")
            return
        coinbase = get_coinbase(block_number)

        epoch_warm.add(coinbase)
        block_warm.add(coinbase)
        tx_warm.add(coinbase)
        ops = df.op.values
        
        tx_hashes = df.tx_hash.values
        contracts = df.contract.values
        slots = df.slot.values
        if "address" in df:
            addresses = df.address.values
        else:
            addresses = []

        sstore_sload_mask = np.isin(ops, ["SSTORE", "SLOAD"])

        rolling_block_sets = {k: set.union(*rolling_block_warm[k]) for k in range(1, 32)}

        last_tx = None
        tx_addresses_cache = {}

        for i in range(n_rows):
            tx_hash = tx_hashes[i]

            if tx_hash != last_tx:
                if tx_hash not in tx_addresses_cache:
                    to_addr = get_to_address(tx_hash, block_number)
                    from_addr = get_from_address(tx_hash, block_number)
                    tx_addresses_cache[tx_hash] = (to_addr, from_addr)
                    block_warm.add(to_addr)
                    block_warm.add(from_addr)
                    epoch_warm.add(to_addr)
                    epoch_warm.add(from_addr)

                tx_warm = set(precompile_addresses)
                tx_warm.add(coinbase)
                to_addr, from_addr = tx_addresses_cache[tx_hash]
                tx_warm.add(to_addr)
                tx_warm.add(from_addr)
                last_tx = tx_hash

            if sstore_sload_mask[i]:
                slot = contracts[i] + "|" + slots[i]
            else:
                slot = addresses[i]
                
            if slot in slot_usage.keys():
                slot_usage[slot].append(f"{tx_hash}|{block_number}")
            else:
                slot_usage[slot] = [f"{tx_hash}|{block_number}"]

            tx_warm_col[i] = slot in tx_warm
            tx_warm.add(slot)

            for k in range(1, 32):
                if k < 31-(skip+1):
                    continue     
                block_warm_roll_cols[k][i] = slot in block_warm or slot in rolling_block_sets[k]

            block_warm_col[i] = slot in block_warm
            if not block_warm_col[i]:
                block_warm.add(slot)

            epoch_warm_col[i] = slot in epoch_warm
            if not epoch_warm_col[i]:
                epoch_warm.add(slot)

        df["tx_warm"] = tx_warm_col
        df["block_warm"] = block_warm_col
        df["epoch_warm"] = epoch_warm_col
        for k in range(1, 32):
            df[f"block_warm_rolling_{k}"] = block_warm_roll_cols[k]

        block_warm_copy = block_warm.copy()
        for k in range(1, 32):
            if k < 31-(skip+1):
                continue          
            rolling_block_warm[k].append(block_warm_copy)
            if len(rolling_block_warm[k]) >= k + 1:
                rolling_block_warm[k] = rolling_block_warm[k][-k:]
            print(f"rolling blocks check: {k}=={len(rolling_block_warm[k])}")

        print("determine_warm_cold_access done")
        return df, epoch_warm, rolling_block_warm

    def determine_warm_cold_access(df):
        global epoch_warm, rolling_block_warm
        print("determine_warm_cold_access...", end="\r")
        tx_warm = set(precompile_addresses)
        block_warm = set(precompile_addresses)
        warmaccess = 0
        coldaccess = 0
        last_tx = None
        block_number = int(df.block_number.iloc[0])
        coinbase = get_coinbase(block_number)
        tx_warm.add(coinbase)
        block_warm.add(coinbase)
        epoch_warm.add(coinbase)
        for i, j in df.iterrows():
            op = j["op"]
            tx_hash = j["tx_hash"]

            to_address = get_to_address(tx_hash, block_number)
            from_address = get_from_address(tx_hash, block_number)
            tx_warm.add(to_address)
            tx_warm.add(from_address)
            block_warm.add(to_address)
            block_warm.add(from_address)
            epoch_warm.add(to_address)
            epoch_warm.add(from_address)

            if last_tx != tx_hash:
                tx_warm = set(precompile_addresses)
                last_tx = tx_hash

            if op in ["SSTORE", "SLOAD"]:
                slot = j["contract"]+j["slot"]

            elif op in ['CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL', 'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH']:
                slot = j["address"]

            if slot in tx_warm:
                df.loc[i, ("tx_warm")] = 1
            else:
                df.loc[i, ("tx_warm")] = 0
                tx_warm.add(slot)
            
            for k in range(1,32):
                if slot in block_warm.union(set.union(*rolling_block_warm[k])):
                    df.loc[i, (f"block_warm_rolling_{str(k)}")] = 1
                else:
                    df.loc[i, (f"block_warm_rolling_{str(k)}")] = 0

            if slot in block_warm:
                df.loc[i, ("block_warm")] = 1
            else:
                df.loc[i, ("block_warm")] = 0
                block_warm.add(slot)
                
            if slot in epoch_warm:
                df.loc[i, ("epoch_warm")] = 1
            else:
                df.loc[i, ("epoch_warm")] = 0
                epoch_warm.add(slot)
                
        for k in range(1, 32):
            print(f"len of rolling blocks ({k}): {len(rolling_block_warm[k])}")
            rolling_block_warm[k].append(block_warm)
            if len(rolling_block_warm[k]) >= k+1:
                rolling_block_warm[k] = rolling_block_warm[k][-k:]
        print("determine_warm_cold_access done")
        return df, epoch_warm, rolling_block_warm

    def compute_storage_savings(row):
        if row['tx_warm'] == 0 and row['block_warm'] == 1:
            if row['op'] == "SLOAD" and row['cost'] == 2100:
                return 2000  
            elif row['op'] == "SSTORE":
                if row['cost'] in [2200, 5000, 22100]:  # Cases where we can save 2100 gas
                    return 2100
                else:
                    return 0  # No savings for other SSTORE costs
        return 0

    def compute_storage_savings_epoch(row):
        if row['tx_warm'] == 0 and row['epoch_warm'] == 1:
            if row['op'] == "SLOAD" and row['cost'] == 2100:
                return 2000  
            elif row['op'] == "SSTORE":
                if row['cost'] in [2200, 5000, 22100]:  # Cases where we can save 2100 gas
                    return 2100
                else:
                    return 0  # No savings for other SSTORE costs
        return 0
    

    def compute_storage_savings_roll(row, k):
        if row['tx_warm'] == 0 and row[f'block_warm_rolling_{k}'] == 1:
            if row['op'] == "SLOAD" and row['cost'] == 2100:
                return 2000  
            elif row['op'] == "SSTORE":
                if row['cost'] in [2200, 5000, 22100]:  # Cases where we can save 2100 gas
                    return 2100
                else:
                    return 0  # No savings for other SSTORE costs
        return 0

    def compute_other_savings(row):
        if row['tx_warm'] == 0 and row['block_warm'] == 1:
            if row['op'] in ['CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL', 'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH']:
                return 2500
        return 0

    def compute_other_savings_epoch(row):
        if row['tx_warm'] == 0 and row['epoch_warm'] == 1:
            if row['op'] in ['CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL', 'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH']:
                return 2500
        return 0
    

    def compute_other_savings_roll(row, k):
        if row['tx_warm'] == 0 and row[f'block_warm_rolling_{k}'] == 1:
            if row['op'] in ['CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL', 'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH']:
                return 2500
        return 0
    
    def compute_savings_fast(df):
        print("compute savings...", end="\r")
        sload_mask = (df['tx_warm'] == 0) & (df['block_warm'] == 1) & (df['op'] == 'SLOAD') & (df['cost'] == 2100)
        sstore_mask = (df['tx_warm'] == 0) & (df['block_warm'] == 1) & (df['op'] == 'SSTORE') & (df['cost'].isin([2200, 5000, 22100]))
        other_mask = (df['tx_warm'] == 0) & (df['block_warm'] == 1) & (df['op'].isin(['CALL', 'CALLCODE', 'DELEGATECALL', 
                                                                                       'STATICCALL', 'BALANCE', 'EXTCODESIZE', 
                                                                                       'EXTCODECOPY', 'EXTCODEHASH']))

        # Compute savings using vectorized assignments
        df['savings'] = 0
        df.loc[sload_mask, 'savings'] = 2000
        df.loc[sstore_mask, 'savings'] = 2100
        df.loc[other_mask, 'savings'] = 2500

        # Repeat for epoch savings
        sload_epoch_mask = (df['tx_warm'] == 0) & (df['epoch_warm'] == 1) & (df['op'] == 'SLOAD') & (df['cost'] == 2100)
        sstore_epoch_mask = (df['tx_warm'] == 0) & (df['epoch_warm'] == 1) & (df['op'] == 'SSTORE') & (df['cost'].isin([2200, 5000, 22100]))
        other_epoch_mask = (df['tx_warm'] == 0) & (df['epoch_warm'] == 1) & (df['op'].isin(['CALL', 'CALLCODE', 'DELEGATECALL', 
                                                                                             'STATICCALL', 'BALANCE', 'EXTCODESIZE', 
                                                                                             'EXTCODECOPY', 'EXTCODEHASH']))

        df['savings_epoch'] = 0
        df.loc[sload_epoch_mask, 'savings_epoch'] = 2000
        df.loc[sstore_epoch_mask, 'savings_epoch'] = 2100
        df.loc[other_epoch_mask, 'savings_epoch'] = 2500

        for k in range(1, 32):
            sload_roll_mask = (df['tx_warm'] == 0) & (df[f'block_warm_rolling_{k}'] == 1) & (df['op'] == 'SLOAD') & (df['cost'] == 2100)
            sstore_roll_mask = (df['tx_warm'] == 0) & (df[f'block_warm_rolling_{k}'] == 1) & (df['op'] == 'SSTORE') & (df['cost'].isin([2200, 5000, 22100]))
            other_roll_mask = (df['tx_warm'] == 0) & (df[f'block_warm_rolling_{k}'] == 1) & (df['op'].isin(['CALL', 'CALLCODE', 
                                                                                                            'DELEGATECALL', 
                                                                                                            'STATICCALL', 'BALANCE', 
                                                                                                            'EXTCODESIZE', 'EXTCODECOPY', 
                                                                                                            'EXTCODEHASH']))

            df[f'savings_roll_{k}'] = 0
            df.loc[sload_roll_mask, f'savings_roll_{k}'] = 2000
            df.loc[sstore_roll_mask, f'savings_roll_{k}'] = 2100
            df.loc[other_roll_mask, f'savings_roll_{k}'] = 2500
            
        sload_mask = (df['op'] == 'SLOAD') & (df['cost'] == 2100)
        sstore_mask = (df['op'] == 'SSTORE') & (df['cost'].isin([2200, 5000, 22100]))
        other_roll_mask = (df['op'].isin(['CALL', 'CALLCODE', 'DELEGATECALL', 'STATICCALL', 'BALANCE', 'EXTCODESIZE', 'EXTCODECOPY', 'EXTCODEHASH']))
        
        df[f'full_saving'] = 0
        df.loc[sload_mask, 'full_saving'] = 2000
        df.loc[sstore_mask, 'full_saving'] = 2100
        df.loc[other_roll_mask, 'full_saving'] = 2500

        print("compute savings done")
        return df



    def merge_block_gas_used(df):
        return pd.merge(df, gas_used, how="left", left_on="block_number", right_on="block_number")

    def compute_savings_of_total(df):
        df["savings_of_total"] = df["savings"] / df.gas_used * 100
        return df
    
    def build_rolling_window_start(block_number):
        global rolling_block_warm
        for i in range(1, 32):
            rolling_block_warm[i] = [set()]
        print("building rolling window to start with")
        for ix, block_number in enumerate(range(block_number-max(rolling_block_warm.keys()), block_number)):
            print(f"building rolling window to start with ({block_number}) | {ix+1}/{max(rolling_block_warm.keys())}")
            df = pd.read_parquet(f"/mnt/hdd1/storage/storage_{block_number}.parquet")
            if "value" in df.columns:
                df.drop("value", inplace=True, axis=1)
            if "tx_index" in df.columns:
                df.drop("tx_index", inplace=True, axis=1)
            df["block_number"] = block_number
            determine_warm_cold_access_fast(df, skip=ix)
        print("------------------------- ROLLING WINDOW BUILT -------------------------")

    try:
        if FROM_SCRATCH:
            raise
        local_df = pd.read_parquet(f"/mnt/hdd1/results/{FILENAME}")
        slot_usage = pd.read_parquet(f"/mnt/hdd1/results/{FILENAME_SLOT}")
        slot_usage["block_number"] = slot_usage["block_number"].apply(lambda x: x.tolist())
        slot_usage = slot_usage.to_dict()["block_number"]
    except:
        local_df = pd.DataFrame(columns=["block_number"])
        slot_usage = dict()

    bl = []
    epoch_warm = set(precompile_addresses)
    current_epoch = None
    start = False
    agg = {
        "cost": "sum",
        "savings": "sum",
        "savings_epoch": "sum",
        "tx_hash": "count",
        "full_saving": "sum"
    }
    toindex=[]
    for k in range(1,32):
        agg[f"savings_roll_{k}"] = "sum"
        toindex.append(f"savings_roll_{k}")
        
    for ix, (df, ii, l) in enumerate(get_files(local_df)):
        if ix >= chunksize:
            break
        if df.empty:
            print("df empty")
            continue
        block_number = int(df.block_number.iloc[0])
        slot = get_slot_of_block_number(block_number)
        if slot % 32 == 0:
            if start == False:
                print("------------------------- STARTING -------------------------")
                build_rolling_window_start(block_number)
                print(f"rolling window now contains {len(rolling_block_warm[max(rolling_block_warm.keys())])} sets")
            start = True
            
        epoch = get_epoch_of_block_number(block_number)
        if current_epoch != epoch:
            print("NEW EPOCH")
            epoch_warm = set(precompile_addresses)
            current_epoch = epoch

        if not start:
            continue
            
        print(f"PARSING {ii}/{chunksize}")
            
        determine_warm_cold_access_fast(df)
        
        smask = df["op"].isin(storage_related_opcodes)
        omask = df["op"].isin(other_opcodes)
         
        compute_savings_fast(df)
        df["slot"] = slot
        df["slot_index"] = slot % 32
              
        df = df.groupby(["block_number", "slot", "slot_index"])[["cost", "savings", "savings_epoch", "full_saving", "tx_hash"]+ toindex].agg(agg).reset_index()
        bl.append(df)


    if len(bl) > 0: 
        df = pd.concat(bl, ignore_index=True)
        print(f"added {df.block_number.nunique()} blocks")
        df = merge_block_gas_used(df)

        df = pd.concat([local_df, df], ignore_index=True)
        df["epoch"] = df["slot"] // 32
        df  = df[df["epoch"] != df.epoch.max()]
        df.drop("epoch", inplace=True, axis=1)
        df.to_parquet(f"/mnt/hdd1/results/{FILENAME}", index=False)
        
        slot_usage = pd.DataFrame.from_dict({
            'block_number': slot_usage
        })
        slot_usage.to_parquet(f"/mnt/hdd1/results/{FILENAME_SLOT}")
        
        print("------------------------- FILE STORED -------------------------")
              
    else:
        df = local_df

    if "slot" in df.columns.tolist():
        df.dropna(subset="slot", inplace=True)
        df["date"] = df.slot.apply(lambda x: xatu.helpers.slot_to_time(x))

    print("------------------------- END ITERATION -------------------------")
    return df


#df = main()
while True:
    print("------------------- STARTING NEW ITERATION -------------------")
    try:
        df = main()
        FROM_SCRATCH = False
        #break
    except KeyboardInterrupt:
        print("------------------- KeyboardInterrupt -------------------")
        break
#!/usr/bin/python2.7
import sys
import pandas as pd
import heapq
import numpy as np

#INPUTS
input_file = sys.argv[1]
zip_output = sys.argv[2]
dt_output = sys.argv[3]

# Data Dictionary
columns = ['CMTE_ID',
'AMNDT_IND',
'RPT_TP',
'TRANSACTION_PGI',
'IMAGE_NUM',
'TRANSACTION_TP',
'ENTITY_TP',
'NAME',
'CITY',
'STATE',
'ZIP_CODE',
'EMPLOYER',
'OCCUPATION',
'TRANSACTION_DT',
'TRANSACTION_AMT',
'OTHER_ID',
'TRAN_ID',
'FILE_NUM',
'MEMO_CD',
'MEMO_TEXT',
'SUB_ID']


def stream_data(input_file, zip_table, dt_table):
    # read file in chunks, and limiting the used columns so we dont load in all the data
    for chunk in pd.read_csv(input_file,
                    sep='|',
                    header=None,
                    names=columns,
                    usecols=['CMTE_ID','ZIP_CODE','TRANSACTION_AMT','TRANSACTION_DT','OTHER_ID'],
                    dtype={'TRANSACTION_DT':'str','ZIP_CODE':'str'},
                    chunksize=1000):
    # pre-filter out the lines that are not allowed
    # Empty OTHER_ID, Has CMTE_ID/TRANSACTION_AMT 
        filtered_chunk = chunk[(pd.isnull(chunk.OTHER_ID) 
                                & pd.notnull(chunk.CMTE_ID) 
                                & pd.notnull(chunk.TRANSACTION_AMT)
                                )]
                                
    # As the data is streamed in, write the output for the zips into a file
        with open(zip_output, 'w') as file:
            for row in filtered_chunk.itertuples():
                zip_data = process_data(zip_table, dt_table, row.CMTE_ID, str(row.ZIP_CODE)[:5], row.TRANSACTION_AMT, str(row.TRANSACTION_DT))
                if zip_data:
                   file.write('|'.join([str(x) for x in zip_data]) + '\n')

# process each line of data and check if zipcode or date is invalid.
# returns zip data output, and aggregastes date data at the same time
def process_data(zip_table, dt_table, cmte_id, zipcode, transaction_amt,transaction_dt):
    processed_zip = []
    if pd.isnull(transaction_dt) or len(transaction_dt)!=8:
        if zipcode != 'nan' and len(zipcode) == 5:
            processed_zip = aggregate_zip(zip_table, cmte_id, zipcode, transaction_amt)
            
    elif pd.isnull(zipcode) or zipcode == 'nan' or len(zipcode)<5:
        if pd.notnull(transaction_dt) or len(transaction_dt)!=8:
            aggregate_dt(dt_table, cmte_id, transaction_dt, transaction_amt)
    else:
        processed_zip = aggregate_zip(zip_table, cmte_id, zipcode, transaction_amt)
        aggregate_dt(dt_table, cmte_id, transaction_dt, transaction_amt)
    return processed_zip

# Aggregate the zip transactions. Returns the data aggregated so far, and keeps history in a dictionary.
# Use tuple of CMTE_ID, ZIP_CODE as the key
def aggregate_zip(table, cmte_id, zipcode, transaction_amt):
    if (cmte_id,zipcode) in table:
        table[(cmte_id,zipcode)]['count'] += 1
        table[(cmte_id,zipcode)]['sum'] += int(transaction_amt)
        add_to_heap(transaction_amt,
                    table[(cmte_id,zipcode)]['lower_heap'],
                    table[(cmte_id,zipcode)]['higher_heap'])
        rebalance_heap(table[(cmte_id,zipcode)]['lower_heap'],
                       table[(cmte_id,zipcode)]['higher_heap'])
        table[(cmte_id,zipcode)]['median'] = get_median(table[(cmte_id,zipcode)]['lower_heap'],
                                                               table[(cmte_id,zipcode)]['higher_heap'])
    else:
        table[(cmte_id,zipcode)] = {
            'higher_heap':[],
            'lower_heap':[],
            'sum':transaction_amt,
            'count':1,
            'median':transaction_amt
        }
        add_to_heap(transaction_amt,
                    table[(cmte_id,zipcode)]['lower_heap'],
                    table[(cmte_id,zipcode)]['higher_heap'])
    return [cmte_id, zipcode, table[(cmte_id,zipcode)]['median'],table[(cmte_id,zipcode)]['count'],
            table[(cmte_id,zipcode)]['sum']]

# Aggregates the date transactions. Doesn't return a result, but updates the date data dictionary.
# Use tuple of CMTE_ID, TRANSACTION_DT as the key
def aggregate_dt(table, cmte_id, transaction_dt, transaction_amt):
    if (cmte_id,transaction_dt) in table:
        table[(cmte_id,transaction_dt)]['count'] += 1
        table[(cmte_id,transaction_dt)]['sum'] += transaction_amt
        add_to_heap(transaction_amt,
                    table[(cmte_id,transaction_dt)]['lower_heap'],
                    table[(cmte_id,transaction_dt)]['higher_heap'])
        rebalance_heap(table[(cmte_id,transaction_dt)]['lower_heap'],
                       table[(cmte_id,transaction_dt)]['higher_heap'])
        table[(cmte_id,transaction_dt)]['median'] = get_median(table[(cmte_id,transaction_dt)]['lower_heap'],
                                                               table[(cmte_id,transaction_dt)]['higher_heap'])
    else:
        table[(cmte_id,transaction_dt)] = {
            'higher_heap':[],
            'lower_heap':[],
            'sum':transaction_amt,
            'count':1,
            'median':transaction_amt
        }
        add_to_heap(transaction_amt,
                    table[(cmte_id,transaction_dt)]['lower_heap'],
                    table[(cmte_id,transaction_dt)]['higher_heap'])

# Use heap implementation of median

# Add value to heaps
def add_to_heap(transaction_amt, lower,higher):
    if len(lower) == 0 or transaction_amt < lower[0]:
        heapq.heappush(lower, transaction_amt)
    else:
        heapq.heappush(higher,transaction_amt)

# Rebalance heaps to maintain heap sizes        
def rebalance_heap(lower,higher):
    if len(lower) > len(higher):
        # heapq package is minheap only, so a negative value heap works around it 
        larger_heap = [x*-1 for x in lower]
        heapq.heapify(larger_heap)
        smaller_heap = higher
        if len(larger_heap) - len(smaller_heap) >= 2:
            heapq.heappush(smaller_heap,heapq.heappop(larger_heap)*-1)
    else:
        larger_heap = higher
        smaller_heap = lower
        if len(larger_heap) - len(smaller_heap) >= 2:
            heapq.heappush(smaller_heap,heapq.heappop(larger_heap))

# Calculate the median using the heaps
def get_median(lower,higher):
    if len(lower) > len(higher):
        larger_heap = [x*-1 for x in lower]
        heapq.heapify(larger_heap)
        smaller_heap = higher
        if len(larger_heap) == len(smaller_heap):
            median = (smaller_heap[0] + larger_heap[0]*-1)/2.0
        elif len(larger_heap) != len(smaller_heap):
            median = larger_heap[0]*-1
    else:
        larger_heap = higher
        smaller_heap = [x*-1 for x in lower]
        heapq.heapify(smaller_heap)
        if len(larger_heap) == len(smaller_heap):
            median = (larger_heap[0] + smaller_heap[0]*-1)/2.0
        elif len(larger_heap) != len(smaller_heap):
            median = larger_heap[0]
    return int(round(median))

#print the date aggregate after the file finishes streaming
def print_date_file(table, dt_output):
    with open(dt_output, 'w') as file:
        for key in sorted(table):
            output = [key[0], key[1], table[key]['median'], table[key]['count'], table[key]['sum']]
            file.write('|'.join([str(x) for x in output]) + '\n')
  
if __name__ == "__main__":
    dt_table = {}
    zip_table = {}
    stream_data(input_file, zip_table, dt_table)
    print_date_file(dt_table, dt_output)
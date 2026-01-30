import clickhouse_connect
from clickhouse_connect.driver.query import QueryContext
import sys
import time

QUERY = "SELECT number FROM system.numbers limit 100000000;"

def select_data_v2(target_path):
    with open(target_path+'/batch_result_v2', 'wb') as f:
        cc_client = clickhouse_connect.get_client(compress=False)
        result = cc_client.query(QUERY)
        with result.row_block_stream as stream:
            for block in stream:
                for row in block:
                    f.write(row[0].to_bytes(8, byteorder='little'))

def select_data(target_path):
   with open(target_path+'/batch_result', 'wb') as f:
       cc_client = clickhouse_connect.get_client(compress=False)
       result = cc_client.query(QUERY)
       for row in result.result_rows:
           f.write(row[0].to_bytes(8, byteorder='little'))

def stream_select_data(target_path):
    with open(target_path+'/stream_result', 'wb') as f:
        cc_client = clickhouse_connect.get_client(compress=False)
        with cc_client.query_row_block_stream(QUERY) as stream:
            for block in stream:
                for row in block:
                    f.write(row[0].to_bytes(8, byteorder='little'))

if __name__ == "__main__":
    target_path = sys.argv[1]
    start_time = time.time()
    stream_select_data(target_path)
    end_time = time.time()
    print(f"Stream select time: {end_time - start_time} seconds")
    
    start_time = time.time()
    select_data(target_path)
    end_time = time.time()
    print(f"Batch select time: {end_time - start_time} seconds")

    start_time = time.time()
    select_data_v2(target_path)
    end_time = time.time()
    print(f"Batch select v2 time: {end_time - start_time} seconds")

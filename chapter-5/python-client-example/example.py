import clickhouse_connect
import sys
import time
import random
import requests

def stream_json(batch_count, batch_size):
    def data_generator():
        yield 'INSERT INTO `test_topic_parsed` (`event_time`, `event_type`, `event_data`) SETTINGS insert_deduplicate=0 FORMAT JSONEachRow'
        cc_client = clickhouse_connect.get_client(compress=False)
        ic = cc_client.create_insert_context(table='test_topic_parsed', column_oriented=True)
        for i in range(batch_count):
            batch_str = ''
            for j in range(batch_size):
                batch_str += f'{{"event_time":{int(time.time())},"event_type":"click","event_data":"{str(random.randint(0, 100))}"}}\n'
            yield batch_str
    requests.post('http://localhost:8123', data=data_generator(), 
                        headers={'User-Agent': 'clickhouse-connect/0.10.0 (lv:py/3.14.2; mode:sync; os:darwin; os_user:jiaqi)', 'Content-Type': 'application/octet-stream'}).text

def batch_json(batch_count, batch_size):
    cc_client = clickhouse_connect.get_client(compress=False)
    ic = cc_client.create_insert_context(table='test_topic_parsed', column_oriented=True)
    for i in range(batch_count):
        batch_str = 'INSERT INTO `test_topic_parsed` (`event_time`, `event_type`, `event_data`) SETTINGS insert_deduplicate=0 FORMAT JSONEachRow'
        for j in range(batch_size):
            batch_str += f'{{"event_time":{int(time.time())},"event_type":"click","event_data":"{str(random.randint(0, 100))}"}}\n'
        requests.post('http://localhost:8123', data=batch_str,
                        headers={'User-Agent': 'clickhouse-connect/0.10.0 (lv:py/3.14.2; mode:sync; os:darwin; os_user:jiaqi)', 'Content-Type': 'application/octet-stream'}).text

def batch_column_oriented(batch_count, batch_size, print_query_id):
    cc_client = clickhouse_connect.get_client(compress=False)
    ic = cc_client.create_insert_context(table='test_topic_parsed', column_oriented=True, settings={'insert_deduplicate': False})
    for i in range(batch_count):
        event_time = []
        event_type = []
        event_data = []
        for j in range(batch_size):
            event_time.append(int(time.time()))
        for j in range(batch_size):
            event_type.append('click')
        for j in range(batch_size):
            event_data.append(str(random.randint(0, 100)))
        ic.data = [event_time, event_type, event_data]
        result = cc_client.insert(context=ic)
        if print_query_id:
            print(result.query_id())

def batch_row_oriented(batch_count, batch_size, print_query_id):
    cc_client = clickhouse_connect.get_client(compress=False)
    ic = cc_client.create_insert_context(table='test_topic_parsed', settings={'insert_deduplicate': False})
    for i in range(batch_count):
        data = []
        for j in range(batch_size):
            data.append([int(time.time()), 'click', str(random.randint(0, 100))])
        ic.data = data
        result = cc_client.insert(context=ic)
        if print_query_id:
            print(result.query_id())

if __name__ == "__main__":
    type = sys.argv[1]
    batch_size = int(sys.argv[2])
    batch_count = int(sys.argv[3])
    print_query_id = False
    if (len(sys.argv) == 5):
        print_query_id = sys.argv[4] == 'true'
    if type == 'column':
        batch_column_oriented(batch_count, batch_size, print_query_id)
    elif type == 'row':
        batch_row_oriented(batch_count, batch_size, print_query_id)
    elif type == 'json_stream':
        stream_json(batch_count, batch_size)
    elif type == 'json':
        batch_json(batch_count, batch_size)
    else:
        raise ValueError(f'Invalid type: {type}')

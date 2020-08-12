'Test the target-ndjson module'

import glob
import os

import singer
import ujson

from target_ndjson import target

def state_log(updated_at='2020-07-05T00:00:00.000000Z'):
    return ujson.dumps({
        'type': 'STATE',
        'value': {
            'bookmarks': {'currently_sync_stream': 'orders', 'orders': {'updated_at': updated_at}}
        }
    })

def schema_log(properties: dict, key_properties: list):
    return ujson.dumps({
        'type': 'SCHEMA',
        'stream': 'orders',
        'schema': {'properties': properties},
        'key_properties': key_properties
    })

def record_log(record: dict):
    return ujson.dumps({
        'type': 'RECORD',
        'stream': 'orders',
        'record': record
    })

class TestTargetNDJSON:
    'Tests the target-ndjson module'

    def teardown_method(self):
        for fpath in glob.glob('orders_*.ndjson'):
            os.remove(fpath)

    def test_parse(self):
        config = {
            'start_date': '2020-01-01',
            'api_key': 'XXX',
            'shop': 'myshop'
        }
        order_schema = schema_log(
            {
                'transaction_amount': {'type': ['null', 'string']},
                'created_at': {'type': 'string'},
                'email': {'type': 'string'}
            },
            ['email']
        )
        order_record = record_log({'transaction_amount': '12.34', 'created_at': '2020-01-01T00:00:00.000000Z', 'email': 'test@mail.com'})
        expected = '''{"transaction_amount":"12.34","created_at":"2020-01-01T00:00:00.000000Z","email":"test@mail.com"}\n'''

        obj = target.TargetNDJSON(config, singer.get_logger())
        obj.persist_lines([order_schema, state_log(), order_record])

        fpaths = glob.glob('orders_*.ndjson')
        assert len(fpaths) == 1
        with open(fpaths[0], 'r') as istream:
            assert expected == istream.read()

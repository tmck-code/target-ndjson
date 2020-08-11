'Test the target-ndjson module'

import singer
from target_ndjson import target

import ujson

class TestTargetNDJSON:
    'Tests the target-ndjson module'

    def setup_class(self):
        self.schema_log = {
            'type': 'SCHEMA',
            'stream': 'orders',
            'schema': {
                'properties': {
                    'transaction_amount': {'type': [ 'null', 'string' ] },
                    'created_at': {'type': 'string'},
                    'email': {'type': 'string'}
                }
            },
            'key_properties': ['email']
        }
        self.state_logs = [
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {'currently_sync_stream': 'orders', 'orders': {'updated_at': '2020-07-05T00:00:00.000000Z'}}
                }
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {'currently_sync_stream': 'orders', 'orders': {'updated_at': '2020-07-30T00:00:00.000000Z'}}
                }
            },
            {
                'type': 'STATE',
                'value': {
                    'bookmarks': {'currently_sync_stream': 'orders', 'orders': {'updated_at': '2020-08-05T00:00:00.000000Z'}}
                }
            }
        ]
        self.record_log = {
            'type': 'RECORD',
            'stream': 'orders',
            'record': {
                'email': 'customer@test.com',
                'created_at': '2020-07-30T01:50:06.000000Z',
                'transaction_amount': '12.23'
            }
        }

    def test_parse(self):
        config = {
            "start_date": "2020-01-01",
            "api_key": "XXX",
            "shop": "myshop"
        }
        json_log_lines = [ujson.dumps(l) for l in [self.schema_log, *self.state_logs, self.record_log]]

        target.TargetNDJSON(config, singer.get_logger()).persist_lines(json_log_lines)

'''
This module writes NDJSON files from Singer Tap logs, aiming to be as fast and
non-transformative as possible during the process
'''

from datetime import datetime
import collections
import sys
import ujson as json

from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error(f'Unable to parse:\n{line}')
            raise

        if 'type' not in o:
            raise Exception(f'Line is missing required key "type": {line}')
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception(f'Line is missing required key "stream": {line}')
            if o['stream'] not in schemas:
                raise Exception(f'A record for stream {o["stream"]} was encountered before a corresponding schema')

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            validators[o['stream']].validate(o['record'])

            # If the record needs to be flattened, uncomment this line
            # flattened_record = flatten(o['record'])

            # TODO: Process Record message here..

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {o["value"]}')
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(f'Line is missing required key "stream": {line}')
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception('key_properties field is required')
            key_properties[stream] = o['key_properties']
        else:
            raise Exception(f'Unknown message type {o["type"]} in message {o}')

    return state




'''
This module writes NDJSON files from Singer Tap logs, aiming to be as fast and
non-transformative as possible during the process
'''

from datetime import datetime
import collections
import sys
import ujson as json

from jsonschema.validators import Draft4Validator

def emit_state(state, logger):
    'Emit state via logger and STDOUT'
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
            items.append((new_key, str(v) if isinstance(v, list) else v))
    return dict(items)

def __fpath_for_stream(stream_name, timestamp):
    return '_'.join([stream_name, timestamp])

def persist_lines(config, lines, logger):
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
            assert isinstance(o, dict)
        except (ValueError, AssertionError):
            logger.error(f'Unable to parse:\n{line}')
            raise

        if 'type' not in o:
            raise Exception(f'Line is missing required key "type": {line}')

        if o['type'] == 'RECORD':
            if 'stream' not in o:
                raise Exception(f'Line is missing required key "stream": {line}')
            if o['stream'] not in schemas:
                raise Exception(f'A record for stream {o["stream"]} was encountered before a corresponding schema')

            # Get schema for this record's stream
            # schema = schemas[o['stream']]
            # Validate record
            validators[o['stream']].validate(o['record'])
            # Write to file
            with open(__fpath_for_stream(o['stream'], now), 'a') as ostream:
                ostream.write(json.dumps(o['record']) + '\n')

            state = None
        elif o['type'] == 'STATE':
            logger.debug('Setting state to {o["value"]}')
            state = o['value']
        elif o['type'] == 'SCHEMA':
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




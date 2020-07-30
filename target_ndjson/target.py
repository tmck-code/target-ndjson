'''
This module writes NDJSON files from Singer Tap logs, aiming to be as fast and
non-transformative as possible during the process
'''

from datetime import datetime
import sys
import ujson

from jsonschema.validators import Draft4Validator

class InvalidTapJSON(ValueError):
    'Raised whenever a Tap output log cannot be parsed'

def emit_state(state, logger):
    'Emit state via logger and STDOUT'
    if state is not None:
        line = ujson.dumps(state)
        logger.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()

def __fpath_for_stream(stream_name, timestamp):
    return '_'.join([stream_name, timestamp])

def persist_lines(_config, lines, logger):
    'Triages and writes the logs lines to physical files on disk'

    state = None
    schemas = {}
    key_properties = {}
    _headers = {}
    validators = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            obj = ujson.loads(line)
            assert isinstance(obj, dict)
        except (ValueError, AssertionError) as e:
            logger.error(f'Unable to parse:\n{line}')
            raise InvalidTapJSON(f'Unable to parse: {e} - {line}')

        if 'type' not in obj:
            raise Exception(f'Line is missing required key "type": {line}')

        if obj['type'] == 'RECORD':
            if 'stream' not in obj:
                raise Exception(f'Line is missing required key "stream": {line}')
            if obj['stream'] not in schemas:
                raise Exception(
                    f'A record for stream {obj["stream"]} was encountered before a '
                    'corresponding schema'
                )

            # Get schema for this record's stream
            # schema = schemas[obj['stream']]
            # Validate record
            validators[obj['stream']].validate(obj['record'])
            # Write to file
            with open(__fpath_for_stream(obj['stream'], now), 'a') as ostream:
                ostream.write(ujson.dumps(obj['record']) + '\n')

            state = None
        elif obj['type'] == 'STATE':
            logger.debug('Setting state to {obj["value"]}')
            state = obj['value']
        elif obj['type'] == 'SCHEMA':
            if 'stream' not in obj:
                raise Exception(f'Line is missing required key "stream": {line}')
            stream = obj['stream']
            schemas[stream] = obj['schema']
            validators[stream] = Draft4Validator(obj['schema'])
            if 'key_properties' not in obj:
                raise Exception('key_properties field is required')
            key_properties[stream] = obj['key_properties']
        else:
            raise Exception(f'Unknown message type {obj["type"]} in message {obj}')

    return state

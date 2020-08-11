'''
This module writes NDJSON files from Singer Tap logs, aiming to be as fast and
non-transformative as possible during the process
'''

from datetime import datetime
import sys
import ujson
from dataclasses import dataclass, field

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

@dataclass
class TargetNDJSON:
    config:         dict
    logger:         object
    state:          str = ''
    schemas:        dict = field(default_factory=dict)
    key_properties: dict = field(default_factory=dict)
    _headers:       dict = field(default_factory=dict)
    validators:     dict = field(default_factory=dict)
    now:            str  = datetime.now().strftime('%Y%m%dT%H%M%S')

    @staticmethod
    def __fpath_for_stream(stream_name, timestamp):
        return '_'.join([stream_name, timestamp])

    def process_line(self, obj):
        if 'type' not in obj:
            raise Exception(f'Line is missing required key "type": {obj}')

        if obj['type'] == 'RECORD':
            if 'stream' not in obj:
                raise Exception(f'Line is missing required key "stream": {obj}')
            if obj['stream'] not in self.schemas:
                raise Exception(
                    f'A record for stream {obj["stream"]} was encountered before a '
                    'corresponding schema'
                )

            # Get schema for this record's stream
            # schema = schemas[obj['stream']]
            # Validate record
            self.validators[obj['stream']].validate(obj['record'])
            # Write to file
            with open(TargetNDJSON.__fpath_for_stream(obj['stream'], self.now), 'a') as ostream:
                ostream.write(ujson.dumps(obj['record']) + '\n')

            self.state = None
        elif obj['type'] == 'STATE':
            print('-----------', self.logger)
            self.logger.debug(f'Setting state to {obj["value"]}')
            self.state = obj['value']
        elif obj['type'] == 'SCHEMA':
            if 'stream' not in obj:
                raise Exception(f'Line is missing required key "stream": {obj}')
            stream = obj['stream']
            self.schemas[stream] = obj['schema']
            self.validators[stream] = Draft4Validator(obj['schema'])
            if 'key_properties' not in obj:
                raise Exception('key_properties field is required')
            self.key_properties[stream] = obj['key_properties']
        else:
            raise Exception(f'Unknown message type {obj["type"]} in message {obj}')

    def persist_lines(self, lines):
        'Triages and writes the logs lines to physical files on disk'

        # Loop over lines from stdin
        for line in lines:
            try:
                obj = ujson.loads(line)
                assert isinstance(obj, dict)
            except (ValueError, AssertionError) as e:
                self.logger.error(f'Unable to parse:\n{line}')
                raise InvalidTapJSON(f'Unable to parse: {e} - {line}')

            self.process_line(obj)
        return self.state

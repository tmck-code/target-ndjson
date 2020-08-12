'''
This module writes NDJSON files from Singer Tap logs, aiming to be as fast and
non-transformative as possible during the process
'''

from datetime import datetime
import sys
import ujson
from dataclasses import dataclass, field
import logging

from jsonschema.validators import Draft4Validator

class InvalidTapJSON(ValueError):
    'Raised whenever a Tap output log cannot be parsed'

def emit_state(state: str, logger: logging.Logger) -> None:
    'Emit state via logger and STDOUT'
    if state:
        line = ujson.dumps(state)
        logger.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()

@dataclass
class TargetNDJSON:
    config:         dict
    logger:         logging.Logger
    state:          str = ''
    schemas:        dict = field(default_factory=dict)
    key_properties: dict = field(default_factory=dict)
    _headers:       dict = field(default_factory=dict)
    validators:     dict = field(default_factory=dict)
    now:            str = datetime.now().strftime('%Y%m%dT%H%M%S')
    file_streams:   dict = field(default_factory=dict)

    @staticmethod
    def __fpath_for_stream(stream_name: str, timestamp: str) -> str:
        return '_'.join([stream_name, timestamp]) + '.ndjson'

    def ensure_files_closed(self) -> None:
        'Ensures that all output files are closed'
        for _stream, ostream in self.file_streams.items():
            ostream.close()

    def process_line(self, obj: dict) -> None:
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
            if obj['stream'] not in self.file_streams:
                self.file_streams[obj['stream']] = open(TargetNDJSON.__fpath_for_stream(obj['stream'], self.now), 'w')

            # Validate record
            self.validators[obj['stream']].validate(obj['record'])
            # Write to file
            self.file_streams[obj['stream']].write(ujson.dumps(obj['record']) + '\n')

            self.state = ''
        elif obj['type'] == 'STATE':
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
                self.ensure_files_closed()
                raise InvalidTapJSON(f'Unable to parse: {e} - {line}')

            try:
                self.process_line(obj)
            except Exception as e:
                self.logger.error('Closing all output files after error: %s', e)
                self.ensure_files_closed()
                raise e
        self.ensure_files_closed()
        return self.state

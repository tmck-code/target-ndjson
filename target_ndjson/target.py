'''
This module writes NDJSON files from Singer Tap logs, aiming to be as fast and
non-transformative as possible during the process
'''

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict
import logging
import sys

from jsonschema.validators import Draft4Validator
import ujson as json

class InvalidTap(Exception):      'Raised if incorrect tap output is encountered'
class InvalidTapJSON(InvalidTap): 'Raised whenever a Tap output log cannot be parsed'
class MissingSchema(InvalidTap):  'Raised if a stream RECORD is encountered before a corresponding SCHEMA line'

@dataclass
class TargetStream:
    'Holds all required metadata in order to process a stream line'

    name:           str
    validator:      Draft4Validator
    schema:         dict
    key_properties: dict
    ostream:        str


@dataclass
class TargetStreamCollection:
    'Holds metadata for all streams that have been encountered'

    streams: Dict[str, TargetStream] = field(default_factory=dict)

    @staticmethod
    def __fpath_for_stream(stream_name: str) -> str:
        return '_'.join([stream_name, datetime.now().strftime('%Y%m%d')]) + '.ndjson'

    def add_stream(self, stream, schema, key_properties):
        self.streams[stream] = TargetStream(
            name=stream,
            validator=Draft4Validator(schema),
            schema=schema,
            key_properties=key_properties,
            ostream=open(TargetStreamCollection.__fpath_for_stream(stream), 'w')
        )

    def get(self, stream):
        if stream not in self.streams:
            raise MissingSchema(f'Stream record for "{stream}" encountered before matching schema')
        return self.streams[stream]

@dataclass
class TargetNDJSON:
    'A NDJSON singer target. Ingests log lines and then writes to destination according to supplied config'

    # TODO destination here
    destination: object
    logger:  logging.Logger
    streams: TargetStreamCollection = TargetStreamCollection()
    state:   str = ''


    def process_line(self, obj: dict) -> None:
        'Checks and processes a parsed log from a tap'

        if obj['type'] == 'SCHEMA':
            self.streams.add_stream(
                stream         = obj['stream'],
                schema         = obj['schema'],
                key_properties = obj['key_properties']
            )

        elif obj['type'] == 'RECORD': 
            self.streams.get(obj['stream']).validator.validate(obj['record'])
            # Data is written/transported here
            self.streams.get(obj['stream']).ostream.write(json.dumps(obj['record']) + '\n')

        elif obj['type'] == 'STATE':
            self.state = obj['value']
        else:
            raise InvalidTap(f'Unknown message type "{obj["type"]}" in message: {obj}')

    @staticmethod
    def parse_line(line) -> dict:
        'Parses a line of JSON tap output'

        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise InvalidTapJSON(f'Unable to parse, expected object: {line}')
        return obj

    def persist_lines(self, lines) -> None:
        'Iterates through log lines and processes each one sequentially'

        for line in lines:
            try:
                self.process_line(TargetNDJSON.parse_line(line))
            except KeyError as e:
                raise InvalidTap(f'Exiting after invalid tap output, missing key "{e}": {line}')
            except Exception as e:
                raise InvalidTap(f'Exiting after unknown error "{e.__class__}": {e}')
        for stream, obj in self.streams.streams.items():
            obj.ostream.close()
        self.emit_state()

    def emit_state(self) -> None:
        'Emit state via logger and STDOUT'

        if self.state:
            sys.stdout.write(f'{json.dumps(self.state)}\n')
            sys.stdout.flush()

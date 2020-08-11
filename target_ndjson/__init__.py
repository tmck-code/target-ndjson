#!/usr/bin/env python3
'This is the entrypoint for target_ndjson'

import argparse
import http.client
import io
import json
import os
import sys
import threading
import urllib
import pkg_resources

import singer
from target_ndjson import target

logger = singer.get_logger()

def send_usage_stats():
    'Send usage stats to singer.io'
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-ndjson',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    # pylint: disable=broad-except
    except Exception:
        logger.debug('Collection request failed')

def main():
    'Parse command-line arguments and run main process'

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    config = {}
    if args.config:
        with open(args.config) as istream:
            config = json.load(istream)

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. '
                    'To disable sending anonymous usage data, set '
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    tap_output = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = target.TargetNDJSON(config, logger).persist_lines(tap_output)

    target.emit_state(state, logger)
    logger.debug('Exiting normally')


if __name__ == '__main__':
    main()

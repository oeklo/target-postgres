#!/usr/bin/env python3

import argparse
import csv
import io
import os
import sys
import json
import re
import threading
import http.client
import urllib
from datetime import datetime
import collections
from tempfile import TemporaryFile

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from target_postgres.db_sync import DbSync

logger = singer.get_logger()


SANITIZE_RE = re.compile(
    r'\\u0000'  # Get rid of JSON encoded null bytes (postgres won't accept null bytes in json)
)


def sanitize_line(line):
    return SANITIZE_RE.sub('', line)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.info('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def new_csv_file_entry():
    csv_f = io.TextIOWrapper(TemporaryFile(mode='w+b'), newline='\n', encoding='UTF-8')
    writer = csv.writer(csv_f)
    return {'file': csv_f, 'writer': writer}


def get_state_changes(prev_state, state):
    result = {}
    if 'bookmarks' in state:
        for key, value in state['bookmarks']:
            prev_value = prev_state.get('bookmarks', {}).get(key)
            if prev_value != value:
                result[key] = value
    return result


def set_bookmark(state, stream, value):
    state['bookmarks'][stream] = value


def persist_lines(config, lines):
    state = None
    last_emitted_state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    csv_files_to_load = {}
    row_count = {}
    stream_to_sync = {}
    primary_key_exists = {}

    # Was renamed from 'batch_size', but it's important to keep the old name
    # for backward compatability.
    max_batch_size = config.get('max_batch_size', config.get('batch_size', 100000))
    # What's the smallest batch size we're ok with? Allows us to output
    # state more eagerly, so we don't lose the work of a stream that fails
    # halfway through.
    min_batch_size = config.get('min_batch_size', max_batch_size)

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    to_flush = set()

    # Loop over lines from stdin
    for line in lines:
        line = sanitize_line(line)
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            # Validate record
            validators[stream].validate(o['record'])

            sync = stream_to_sync[stream]

            primary_key_string = sync.record_primary_key_string(o['record'])
            if stream not in primary_key_exists:
                primary_key_exists[stream] = {}
            if primary_key_string and primary_key_string in primary_key_exists[stream]:
                to_flush.add(stream)

            writer = csv_files_to_load[o['stream']]['writer']
            writer.writerow(sync.record_to_csv_row(o['record']))
            row_count[o['stream']] += 1
            if primary_key_string:
                primary_key_exists[stream][primary_key_string] = True

        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            if stream not in schemas:
                # If we get more than one schema message for the same stream,
                # only use the first. Otherwise, this will clobber any
                # records we've collected for this schema so far.
                schemas[stream] = o
                validators[stream] = Draft4Validator(o['schema'])
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")
                key_properties[stream] = o['key_properties']
                stream_to_sync[stream] = DbSync(config, o)
                stream_to_sync[stream].create_schema_if_not_exists()
                stream_to_sync[stream].sync_table()
                row_count[stream] = 0
                csv_files_to_load[stream] = new_csv_file_entry()
            else:
                logger.warning('more than one schema message found for stream %r; only the first will be used', stream)

        elif t == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')

        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

        # Decide which streams to flush, if any.
        changes = get_state_changes(last_emitted_state, state)
        write_bookmark = False
        for stream, sync in stream_to_sync.items():
            count = row_count[stream]
            do_flush = stream in to_flush
            if count >= max_batch_size:
                do_flush = True
            elif stream in changes:
                if count == 0 or count >= min_batch_size:
                    do_flush = True

            if do_flush:
                if row_count != 0:
                    flush_records(stream, csv_files_to_load, row_count, primary_key_exists, sync)
                if stream in changes:
                    write_bookmark = True
                    set_bookmark(last_emitted_state, stream, changes[stream])

        if write_bookmark:
            emit_state(last_emitted_state)

    for (stream_name, count) in row_count.items():
        if count > 0:
            flush_records(stream_name, csv_files_to_load, count, primary_key_exists, sync)

    emit_state(state)


def flush_records(stream, csv_files_to_load, row_count, primary_key_exists, sync):
    csv_files_to_load[stream]['file'].flush()
    # Convert the file to the underlying binary file.
    csv_file = csv_files_to_load[stream]['file'].detach()
    sync.load_csv(csv_file, row_count[stream])
    row_count[stream] = 0
    primary_key_exists[stream] = {}
    csv_files_to_load[stream] = new_csv_file_entry()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_lines(config, input)

    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()

#!/usr/bin/env python

import os
import csv
import sys
import glob
import time
import logging
import datetime
import psycopg2 as pg
from conf import config

## helpers ###################################################################

def get_files(masks, since=None, sort=None):
    if type(masks) is str:
        masks = [masks]

    gen = ({'name': os.path.abspath(file), 'time': os.path.getmtime(file)}
            for mask in masks for file in glob.glob(os.path.join(config.root, mask)) if not file.endswith('.error'))

    if since is not None:
        gen = filter(lambda x: x['time'] > since, gen)

    if sort is None: return list(gen)
    else: return sorted(gen, key=lambda x: x[sort])

## action ####################################################################

class ActionMeta(type):
    def __init__(cls, name, bases, nmspc):
        super().__init__(name, bases, nmspc)
        cls._id = cls._id if cls._id else cls._table
        cls._before = set(cls._before)
        cls._after = set(cls._after)

class Action(metaclass = ActionMeta):
    '''
    The action happens in three stages: loading, staging, and insert.
    This allows the ETL to be precise and separated, as custom logic allow
    staged rows to be manipulated before they enter the main table.
    Examples: a data file may completely dismiss previously loaded rows
    '''

    ## configuration
    _id, _table = '', ''
    _schema = config.schema
    _encoding = config.encoding
    _files = []
    _keys = []
    _fields = []
    _update = False

    _before = set()
    _after = set()

    def transform(self, line): return line

    ## helper
    def __init__(self, conn, complete=False):
        self.log = logging.getLogger(self._id)
        self.conn = conn
        self.complete = complete
        if not self._fields:
            self.sql('SELECT column_name FROM information_schema.columns WHERE (table_schema, table_name) = (%s, %s) ORDER BY ordinal_position', [self._schema, self._table])
            self._fields = [col[0] for col in self.conn.fetchall()]

    def sql(self, cmd, args=None, level=15):
        self.log.getChild('sql').log(level, '> %s', self.conn.mogrify(cmd, args).decode('utf-8'))
        self.conn.execute(cmd, args)

    def has_key(self): return len(self._keys) > 0
    def key(self): return ', '.join(self._keys)
    def field_to_pos(self, name): return self._fields.index(name)
    def field(self): return ', '.join(self._fields)
    def field_set(self, table='EXCLUDED'): return ', '.join('{0} = {1}.{0}'.format(i, table) for i in self._fields)

    def last_updated(self, value=None):
        if value is None:
            self.sql('SELECT updated FROM _updated WHERE id = %s', [self._id])
            value = self.conn.fetchone()
            return -1 if value is None else (time.mktime(value[0].timetuple()) + value[0].microsecond / 1e6)
        else:
            value = datetime.datetime.fromtimestamp(value, self.conn.tzinfo)
            self.sql('INSERT INTO _updated VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET updated = EXCLUDED.updated', [self._id, value])
            return value

    def conflict_phrase(self):
        if not self.has_key(): return ''
        else: return 'ON CONFLICT ({}) DO {}'.format(self.key(), 'UPDATE SET {}'.format(self.field_set()) if self._update else 'NOTHING')

    ## stages
    def before_run(self): pass
    def before_setup(self): pass
    def after_setup(self): pass
    def before_files(self): pass
    def after_files(self): pass
    def before_staging(self): pass
    def before_load(self): pass
    def after_load(self): pass
    def after_staging(self): pass
    def before_insert(self): pass
    def after_insert(self): pass
    def after_run(self): pass

    ## perform
    def do_setup(self):
        self.fields = '({})'.format(self.field()) if self._fields else ''
        self.distinct, self.conflict = '', ''
        if self.has_key():
            self.distinct = 'DISTINCT ON ({})'.format(self.key())
            self.conflict = self.conflict_phrase()

    def do_files(self):
        self.since = self.last_updated() if not self.complete else -1
        self.files = get_files(self._files, sort='time', since=self.since)

    def do_staging(self):
        self.count = 0
        self.latest = self.since
        self.before_staging()

        for record in self.files:
            fname, changed = record['name'], record['time']
            if self.latest < changed:
                self.latest = changed
            self.do_load(fname)

        self.after_staging()
        self.last_updated(self.latest)

    def do_load(self, fname):
        self.sql('CREATE TEMP TABLE imported (LIKE {}.{}) ON COMMIT DROP'.format(self._schema, self._table))

        self.before_load()
        total = 0
        try:
            with open(fname, 'r', encoding=self._encoding) as file:
                reader = csv.reader(file)
                try: count = len(next(reader))
                except StopIteration: pass
                else:
                    reader = (line for line in (self.transform(line) for line in reader if len(line) > 0) if line is not None)
                    while True:
                        lines = zip(range(1,500), reader)
                        values = ','.join(self.conn.mogrify('({})'.format(','.join(['%s'] * len(line))), line).decode('utf-8') for i, line in lines)
                        if values == '': break
                        self.sql('INSERT INTO imported ({}) VALUES {}'.format(self.field(), values, self.conflict), level=logging.DEBUG)
                        total += self.conn.rowcount
        except pg.Error as e:
            os.replace(fname, fname + '.error')
            raise e
        else:
            if os.path.isfile(fname + '.error'):
                os.remove(fname + '.error')
        self.after_load()

        if total > 0:
            self.sql('INSERT INTO staging SELECT {} * FROM imported {}'.format(self.distinct, self.conflict))

        self.log.info('Loaded %s rows from %s.', total, fname)
        self.count += total
        self.sql('DROP TABLE imported')

    def run(self):
        self.before_run()

        self.before_setup()
        self.do_setup()
        self.after_setup()

        self.before_files()
        self.do_files()
        self.after_files()

        if len(self.files) == 0: self.log.info('No files after %s - skipped.', datetime.datetime.fromtimestamp(self.since, self.conn.tzinfo))
        else:
            self.sql('CREATE TEMP TABLE staging (LIKE {}) ON COMMIT DROP'.format(self._table))
            if self.has_key(): self.sql('CREATE UNIQUE INDEX ON staging ({})'.format(self.key()))
            self.do_staging()
            self.before_insert()
            self.sql('INSERT INTO {}.{} SELECT * FROM staging {}'.format(self._schema, self._table, self.conflict))
            self.after_insert()
            self.log.info('Processed %s rows from %s files.', self.count, len(self.files))
            self.sql('DROP TABLE staging')

        self.after_run()
        self.log.info('Done.')

#!/usr/bin/env python

import logging
from .source.base import NullSource

## meta ######################################################################

def decorate_stage(stage, do):
    '''Hooks the do_* functions with before_ and after_'''
    if stage == 'staging':
        def hook(self, *args, **kwargs):
            getattr(self, 'before_' + stage)(*args, **kwargs)
            val = do(self, *args, **kwargs)
            getattr(self, 'after_' + stage)(*args, **kwargs)
            return val
    else:
        def hook(self, *args, **kwargs):
            log = self.log
            self.log = log.getChild(stage)
            getattr(self, 'before_' + stage)(*args, **kwargs)
            val = do(self, *args, **kwargs)
            getattr(self, 'after_' + stage)(*args, **kwargs)
            self.log = log
            return val
    return hook

registry = { }
class ActionMeta(type):
    def __new__(cls, name, bases, nmspc):
        for key in nmspc:
            if not key.startswith('do_'): continue
            _, stage = key.split('_', 2)
            nmspc[key] = decorate_stage(stage, nmspc[key])

        return type.__new__(cls, name, bases, nmspc)

    def __init__(cls, name, bases, nmspc):
        log = logging.getLogger('action')

        super().__init__(name, bases, nmspc)

        # sanitize class definition
        cls._id = cls._id if cls._id else cls._table
        cls._before = set(cls._before)
        cls._after = set(cls._after)
        cls.after_declare(name, bases, nmspc)

        if cls._id:
            if cls._id in registry:
                log.warning('Duplicate action found: %s (%s, %s)',
                    cls._id, registry.memo[cls._id], cls)
            registry[cls._id] = cls

        if '_template' not in cls.__dict__:
            available = list(bases)
            defined = set()
            while available:
                base = available.pop(0)
                defined.update(base.__dict__.keys())
                available.extend(base.__bases__)
            unknown = set(cls.__dict__.keys()) - defined
            if len(unknown) > 0:
                log.warning('%s.%s has unsupported features: %s',
                    nmspc['__module__'], name, ', '.join(sorted(unknown)))
        else:
            del cls._template

## base ######################################################################

class Action(metaclass=ActionMeta):
    '''
    Each action happens in two stages: extract and load.
    This happens separately for each source.
    This allows the ETL to be precise and separated, as custom logic allow
    staged rows to be manipulated before they enter the main table.
    '''

    _template = True

    ## configuration
    _id = ''
    _schema, _table = '', ''
    _source = NullSource()
    _keys = []
    _fields = []
    _update = False

    _before = set()
    _after = set()

    def transform(self, entry, line): return line

    ## helper
    def __init__(self, conn, complete=False):
        self.log = logging.getLogger(self._id)
        self.conn = conn
        self.complete = complete
        if not self._fields:
            self.sql('SELECT column_name FROM information_schema.columns '
                'WHERE (table_schema, table_name) = (%s, %s) '
                'ORDER BY ordinal_position',
                [self._schema, self._table])
            self._fields = [col[0] for col in self.conn.fetchall()]

    def sql(self, cmd, args=None, level=15):
        self.log.getChild('sql').log(level, '> %s',
            self.conn.mogrify(cmd, args).decode('utf-8'))
        self.conn.execute(cmd, args)

    def has_key(self): return len(self._keys) > 0
    def key(self): return ', '.join(self._keys)

    def field_to_pos(self, name): return self._fields.index(name)
    def field(self): return ', '.join(self._fields)
    def field_set(self, table='EXCLUDED'):
        return ', '.join('{0} = {1}.{0}'.format(i, table) for i in self._fields)

    def last_updated(self, value=None):
        if value is None:
            self.sql('SELECT updated FROM _updated WHERE id = %s', [self._id])
            return self.conn.fetchone()[0]
        else:
            self.sql('INSERT INTO _updated VALUES (%s, %s) '
                'ON CONFLICT (id) DO UPDATE SET updated = EXCLUDED.updated',
                [self._id, value])
            return value

    def conflict_phrase(self):
        if not self.has_key(): return ''
        action = 'UPDATE SET {}'.format(self.field_set() if self._update else 'NOTHING')
        return 'ON CONFLICT ({}) DO {}'.format(self.key(), action)

    ## stages
    @classmethod
    def after_declare(cls, name, bases, nmspc): pass
    def before_run(self): pass
    def before_setup(self): pass
    def after_setup(self): pass
    def before_list(self): pass
    def after_list(self): pass
    def before_staging(self): pass
    def before_extract(self, entry): pass
    def after_extract(self, entry): pass
    def before_load(self, entry): pass
    def after_load(self, entry): pass
    def after_staging(self): pass
    def after_run(self, skipped): pass

    ## perform
    def do_setup(self):
        self.since = self.last_updated()
        self.conflict = self.conflict_phrase()

    def do_list(self):
        self.list = self._source.list(self.since, self.complete)
        self.log.info('Generated %s sources.', len(self.list))

    def do_staging(self):
        for record in self.list:
            entry, updated, meta = record
            self.latest = updated

            total = self.do_extract(entry)
            self.extracted += total

            if total > 0:
                self.loaded += self.do_load(entry)

            self.sql('TRUNCATE staging')


    def do_extract(self, entry):
        self.log.debug('Extracting data from %s.', entry)
        total, source = 0, (self.transform(entry, line) for line in self._source.extract(entry))
        while True:
            lines = list(zip(range(500), source))
            if len(lines) == 0: break

            values = ','.join(self.conn.mogrify('({})'.format(','.join(['%s'] * len(line))), line).decode('utf-8') for i, line in lines)
            try:
                self.sql('INSERT INTO staging ({}) VALUES {} {}'.format(self.field(), values, self.conflict), level=logging.DEBUG)
            except Exception as e:
                self.log.error('Failed to import %s somewhere between rows #%s and #%s', entry, total+1, total+len(lines))
                self._source.fail(entry, e)
                raise e
            else:
                total += self.conn.rowcount
                self.log.debug('Loaded %s (+%s) rows.', total, self.conn.rowcount)

        self._source.succeed(entry)
        self.log.info('Extracted %s rows from %s.', total, entry)
        return total

    def do_load(self, entry):
        self.sql('INSERT INTO {}.{} SELECT * FROM staging {}'.format(self._schema, self._table, self.conflict))
        self.log.info('Loaded %s rows from %s.', self.conn.rowcount, entry)
        return self.conn.rowcount

    def run(self):
        self.before_run()

        self.do_setup()
        self.do_list()

        skipped = False
        if len(self.list) == 0:
            skipped = True
            self.log.info('Found no new sources since %s -- skipped.', self.since.astimezone(tz=None))
        else:
            self.sql('CREATE TEMP TABLE staging (LIKE {}.{}) ON COMMIT DROP'.format(self._schema, self._table))
            if self.has_key(): self.sql('CREATE UNIQUE INDEX ON staging ({})'.format(self.key()))

            self.extracted, self.loaded = 0, 0
            self.do_staging()

            self.last_updated(self.latest)
            if self.extracted != self.loaded:
                self.log.info('Processed %s (%s) rows from %s files.', self.loaded, '{:+}'.format(self.extracted - self.loaded), len(self.list))
            else:
                self.log.info('Processed %s rows from %s files.', self.loaded, len(self.list))
            self.sql('DROP TABLE staging')

        self.after_run(skipped)
        if not skipped: self.log.info('Done.')

#!/usr/bin/env python

import logging
from .source.base import NullSource

## meta ######################################################################

registry = { }
class ActionMeta(type):
    def __init__(cls, name, bases, nmspc):
        log = logging.getLogger('action')

        super().__init__(name, bases, nmspc)
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

    def transform(self, line): return line

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
    def before_extract(self, entry): pass
    def after_extract(self, entry): pass
    def before_insert(self, entry): pass
    def after_insert(self, entry): pass
    def after_run(self, skipped): pass

    ## perform
    def do_setup(self):
        self.since = self.last_updated()
        self.conflict = self.conflict_phrase()

    def do_list(self):
        self.list = self._source.list(self.since, self.complete)

    def do_extract(self, entry):
        total, source = 0, self._source.extract(entry)
        while True:
            lines = list(zip(range(500), source))
            if len(lines) == 0: break

            values = ','.join(self.conn.mogrify('({})'.format(','.join(['%s'] * len(line))), line).decode('utf-8') for i, line in lines)
            try:
                self.sql('INSERT INTO staging ({}) VALUES {} {}'.format(self.field(), values, self.conflict), level=logging.DEBUG)
            except Exception as e:
                self._source.fail(entry, e)
                self.log.error('Failed to import %s somewhere between rows #%s and #%s.', entry, total, total+len(lines))
                raise e
            else:
                total += self.conn.rowcount

        self._source.succeed(entry)
        self.log.info('Extracted %s rows from %s.', total, entry)
        return total

    def do_insert(self):
        self.sql('INSERT INTO {}.{} SELECT * FROM staging {}'.format(self._schema, self._table, self.conflict))

    def run(self):
        self.before_run()

        self.before_setup()
        self.do_setup()
        self.after_setup()

        self.before_list()
        self.do_list()
        self.after_list()

        skipped = False
        if len(self.list) == 0:
            skipped = True
            self.log.info('Found no new sources since %s -- skipped.', self.since.astimezone(tz=None))
        else:
            self.sql('CREATE TEMP TABLE staging (LIKE {}.{}) ON COMMIT DROP'.format(self._schema, self._table))
            if self.has_key(): self.sql('CREATE UNIQUE INDEX ON staging ({})'.format(self.key()))

            self.total = 0
            for record in self.list:
                entry, updated, meta = record
                self.latest = updated

                self.before_extract(entry)
                total = self.do_extract(entry)
                self.total += total
                self.after_extract(entry)

                if total > 0:
                    self.before_insert(entry)
                    self.do_insert()
                    self.after_insert(entry)

                self.sql('TRUNCATE staging')

            self.last_updated(self.latest)
            self.log.info('Processed %s rows from %s files.', self.total, len(self.list))
            self.sql('DROP TABLE staging')

        self.after_run(skipped)
        if not skipped: self.log.info('Done.')

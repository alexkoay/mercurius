import psycopg2 as pg

class Connection(pg.extensions.connection):
    tzinfo = None
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def cursor(self, *args, **kwargs):
        cur = super().cursor(*args, **kwargs)
        if self.tzinfo is None:
            cur.execute('select current_timestamp')
            self.tzinfo = cur.fetchone()[0].tzinfo

        cur.tzinfo = self.tzinfo
        return cur

class Cursor(pg.extensions.cursor):
    tzinfo = None

def connect(*args, **kwargs):
    return pg.connect(*args, connection_factory=Connection, cursor_factory=Cursor, **kwargs)
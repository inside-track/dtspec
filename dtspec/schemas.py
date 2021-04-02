import sqlalchemy as sa
from sqlalchemy.engine import reflection

import snowflake.sqlalchemy

from dtspec.decorators import retry

class UnknownEngineTypeError(Exception): pass

def generate_engine(engine_type, host, port=None, user=None, password=None, dbname=None, warehouse=None, role=None):
    if engine_type == 'postgres':
        return sa.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    if engine_type == 'snowflake':
        return sa.create_engine(
            snowflake.sqlalchemy.URL(
                account=host,
                user=user,
                password=password,
                database=dbname,
                schema='public',
                warehouse=warehouse,
                role=role
            )
        )
    raise UnknownEngineTypeError(f'Unsupported engine type: {engine_type}')


class SchemaReflector:
    def __init__(self, engine, namespace='public', tables=None):
        self.engine = engine
        self.dbname = engine.url.database
        self.namespace = namespace
        self.tables = tables or []
        self.metadata = sa.MetaData()

    @retry(sa.exc.InternalError, delay=0)
    def fetch_table_names(self):
        insp = reflection.Inspector.from_engine(self.engine)
        views = list(insp.get_view_names(schema=self.namespace))
        return self.engine.table_names(schema=self.namespace) + views

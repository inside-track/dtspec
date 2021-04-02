import asyncio
from concurrent.futures import ThreadPoolExecutor

import yaml
import nest_asyncio
import sqlalchemy as sa
from sqlalchemy.engine import reflection

import snowflake.sqlalchemy

from dtspec.log import LOG
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


def reflect(engine, namespace='public', tables=None):
    tables = tables or []
    metadata = sa.MetaData()

    reflected_table_names = _reflect_table_names(engine, namespace)
    selected_table_names = _select_tables(tables, reflected_table_names)
    LOG.debug('Reflecting tables: %s', selected_table_names)

    _reflect_tables(metadata, engine, namespace, selected_table_names)

    print(_metadata_yaml(metadata))



@retry(sa.exc.InternalError, delay=0)
def _reflect_table_names(engine, namespace):
    insp = reflection.Inspector.from_engine(engine)
    views = list(insp.get_view_names(schema=namespace))
    return engine.table_names(schema=namespace) + views

def _select_tables(user_tables, reflected_table_names):
    if user_tables == '*':
        return reflected_table_names
    return list(set(user_tables) & set(reflected_table_names))

@retry(sa.exc.InternalError, delay=0)
def _reflect_table(metadata, engine, namespace, table_name):
    LOG.info('Reflecting table %s.%s', namespace, table_name)
    sa_table = sa.Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine,
        schema=namespace,
        resolve_fks=False,
    )

def _reflect_tables(metadata, engine, namespace, table_names):
    async def async_reflect_tables(table_names):
        with ThreadPoolExecutor(max_workers=8) as executer:
            event_loop = asyncio.get_event_loop()
            tasks = [
                event_loop.run_in_executor(executer, _reflect_table, metadata, engine, namespace, table_name)
                for table_name in table_names
            ]

            for task in tasks:
                await asyncio.gather(task)

    nest_asyncio.apply()
    asyncio.get_event_loop().run_until_complete(async_reflect_tables(table_names))

def _metadata_yaml(metadata):
    schema = {}
    for table_name, sa_table in metadata.tables.items():
        table_name = table_name.split('.')[1]
        schema[table_name] = []

        for col in sa_table.columns:
            col_meta = {
                'name': str(col.name),
                'type': col.type.copy(),
                'primary_key': col.primary_key,
                'nullable': col.nullable,
                'default': col.default,
            }
            schema[table_name].append(col_meta)

    return yaml.dump(schema, default_flow_style=False, explicit_start=True)


# TODO: write/read yaml file!!

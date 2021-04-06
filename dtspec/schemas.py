import os
import glob
import re
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

def execute_sqls(engine, sqls, max_workers=4):
    '''
    Used to run a list of sql commands distributed over a number of threads.
    This method splits sql into a number of batches (max_workers) and executes
    SQL for each batch inside of a single database transaction.
    There is no guarantee that the SQL will run in any specific order.

    Args:
      engine - SQLAlchemy engine
      sql - List of sql statements to run
      max_workers - Maximum number of parallel threads to run
    '''

    async def async_execute_sqls(worker_execute_sqls, engine, sqls, max_workers=max_workers):
        worker_batch_sqls = [sqls[iworker::max_workers] for iworker in range(max_workers)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            event_loop = asyncio.get_event_loop()
            tasks = [
                event_loop.run_in_executor(executor, worker_execute_sqls, engine, worker_sqls)
                for worker_sqls in worker_batch_sqls
            ]

            for task in tasks:
                await asyncio.gather(task)

    def worker_execute_sqls(engine, worker_sqls):
        with engine.connect().begin() as trans:
            for worker_sql in worker_sqls:
                LOG.debug('Executing sql: %s', worker_sql)
                trans.connection.execute(worker_sql)
            trans.commit()

    nest_asyncio.apply()
    asyncio.get_event_loop().run_until_complete(
        async_execute_sqls(worker_execute_sqls, engine, sqls)
    )


def reflect(env, engine, output_path, namespace='public', tables=None):
    tables = tables or []
    metadata = sa.MetaData()

    reflected_table_names = _reflect_table_names(engine, namespace)
    selected_table_names = _select_tables(tables, reflected_table_names)
    LOG.debug('Reflecting tables: %s', selected_table_names)

    _reflect_tables(metadata, engine, namespace, selected_table_names)
    _write_yaml(output_path, env, namespace, metadata)


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

def _schema_yaml(metadata):
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

def _write_yaml(output_path, env, namespace, metadata):
    schema_yaml = _schema_yaml(metadata)

    yaml_file = os.path.join(output_path, f'{env}.{namespace}.schema.yml')
    with open(yaml_file, 'w') as yfile:
          yfile.write(
              '\n'.join(
                  [
                      '# This yaml file is autogenerated by reflecting the schema from live databases.',
                      '# It should not be edited by hand, because future generations will overwrite it.',
                  ]
              )
              + '\n'
          )
          yfile.write(schema_yaml)

def _read_yaml(env, schema_path):
    schema_yaml = {}
    for yaml_file in glob.glob(os.path.join(schema_path, f'{env}.*.schema.yml')):
        LOG.info('Parsing schema defined in %s', yaml_file)
        namespace = re.search(fr'{env}.(.+).schema.yml', yaml_file).group(1)
        with open(yaml_file, 'r') as yfile:
            yaml_txt = yfile.read()
        schema_yaml[namespace] = yaml.unsafe_load(yaml_txt)

    return schema_yaml

def _reconstruct_sa_metadata(schema):
    metadata = sa.MetaData()

    return {
        namespace: {
            table_name: sa.Table(
                table_name,
                metadata,
                *[
                    sa.Column(
                        col['name'],
                        col['type'],
                        primary_key=col['primary_key'],
                        nullable=col['nullable'],
                        default=col['default'],
                    )
                    for col in columns
                ],
                schema=namespace
            )
            for table_name, columns in tables.items()
        }
        for namespace, tables in schema.items()
    }

def _create_table_sql(table, engine):
    return str(sa.schema.CreateTable(table).compile(engine)) + ';'

def _create_namespace_sql(namespace, clean=False):
    sql = ''
    if clean:
        sql += f'DROP SCHEMA IF EXISTS {namespace} CASCADE; '
    sql += f'CREATE SCHEMA {namespace}; '
    return sql

def init_test_db(env, engine, schema_path, clean=False):
    schema_yaml = _read_yaml(env, schema_path)
    schema_metadata = _reconstruct_sa_metadata(schema_yaml)


    create_schema_sqls = [
        _create_namespace_sql(namespace, clean=clean)
        for namespace in schema_metadata.keys()
    ]

    create_table_sqls = [
        _create_table_sql(table, engine)
        for namespace, tables in schema_metadata.items()
        for table_name, table in tables.items()
    ]

    execute_sqls(engine, create_schema_sqls)
    execute_sqls(engine, create_table_sqls)

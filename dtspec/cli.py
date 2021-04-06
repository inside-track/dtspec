import os
import json
import argparse
import pathlib

import yaml
import jinja2
import dtspec

import dtspec.schemas
from dtspec.log import LOG

# TODO: Document environment variable

DTSPEC_ROOT=os.path.join(os.getcwd(), 'dtspec')
if 'DTSPEC_ROOT' in os.environ:
    DTSPEC_ROOT=os.environ['DTSPEC_ROOT']

PARSER = argparse.ArgumentParser(description='dtspec cli')
PARSER.add_argument('--env', dest='env', default='default')
PARSER.add_argument('--fetch-schemas', dest='fetch_schemas', const=True, nargs='?', default=False)
PARSER.add_argument('--init-test-db', dest='init_test_db', const=True, nargs='?', default=False)
PARSER.add_argument('--clean', dest='clean', const=True, nargs='?', default=False)


def get_config():
    template_loader = jinja2.FileSystemLoader(searchpath=DTSPEC_ROOT)
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template('config.yml')
    rendered_template = template.render(
        env_var=lambda var, default='': os.environ.get(var, default)
    )
    config = yaml.safe_load(rendered_template)

    return config

def main():
    args = PARSER.parse_args()
    config = get_config()

    if args.fetch_schemas:
        fetch_schemas(config, args.env)
        return

    if args.init_test_db:
        init_test_db(config, args.env, args.clean)
        return


def _engine_from_config(schema_config):
    return dtspec.schemas.generate_engine(
        engine_type=schema_config['type'],
        host=schema_config['host'],
        port=schema_config.get('port'),
        user=schema_config['user'],
        password=schema_config['password'],
        dbname=schema_config['dbname'],
        warehouse=schema_config.get('warehouse'),
        role=schema_config.get('role'),
    )


def fetch_schemas(config, env):
    LOG.info('fetching schemas for env: %s', env)

    schema_config = config['environments'][env]['schema']
    engine = _engine_from_config(schema_config)

    output_path = os.path.join(DTSPEC_ROOT, 'schemas')
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    for namespace, tables in schema_config['tables'].items():
        dtspec.schemas.reflect(
            env=env,
            engine=engine,
            output_path=output_path,
            namespace=namespace,
            tables=tables,
        )

def init_test_db(config, env, clean=False):
    LOG.info('initializing test db env: %s', env)
    schema_config = config['environments'][env]['test']
    engine = _engine_from_config(schema_config)
    schemas_path = os.path.join(DTSPEC_ROOT, 'schemas')

    dtspec.schemas.init_test_db(
        env=env,
        engine=engine,
        schema_path=schemas_path,
        clean=clean
    )

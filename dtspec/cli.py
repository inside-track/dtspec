import os
import json
import argparse

import yaml
import jinja2
import dtspec

import dtspec.schemas

# TODO: Document environment variable
DTSPEC_CONFIG_PATH=os.path.join(os.getcwd(), 'dtspec/config.yml')
if 'DTSPEC_CONFIG_PATH' in os.environ:
    DTSPEC_CONFIG_PATH=os.environ['DTSPEC_CONFIG_PATH']

PARSER = argparse.ArgumentParser(description='dtspec cli')
PARSER.add_argument('--env', dest='env', default='default')
PARSER.add_argument('--fetch-schemas', dest='fetch_schemas', const=True, nargs='?', default=False)


def get_config():
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(DTSPEC_CONFIG_PATH))
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(os.path.split(DTSPEC_CONFIG_PATH)[1])
    rendered_template = template.render(
        env_var=lambda var, default='': os.environ.get(var, default)
    )
    config = yaml.safe_load(rendered_template)

    return config

def main():
    args = PARSER.parse_args()

#    print('hello from dtspec')
#    print(f'args: {args}')
    config = get_config()
#    print(json.dumps(config, indent=4))

    if args.fetch_schemas:
        fetch_schemas(config, args.env)
        return


def fetch_schemas(config, env):
    print(f'fetching schemas for env {env}')
#    print(json.dumps(config['environments'][env], indent=4))

    schema_config = config['environments'][env]['schema']
    engine = dtspec.schemas.generate_engine(
        engine_type=schema_config['type'],
        host=schema_config['host'],
        port=schema_config.get('port'),
        user=schema_config['user'],
        password=schema_config['password'],
        dbname=schema_config['dbname'],
        warehouse=schema_config.get('warehouse'),
        role=schema_config.get('role'),
    )

    for namespace, tables in schema_config['tables'].items():
        reflector = dtspec.schemas.SchemaReflector(
            engine=engine,
            namespace=namespace,
            tables=tables
        )

        print(reflector.fetch_table_names())

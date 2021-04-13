import os
import json
import argparse
import pathlib

import yaml
import jinja2
import dtspec

import dtspec.db
import dtspec.specs
import dtspec.shell

from dtspec.log import LOG

# TODOC: specs must all live in a specs subdirectory
# TODO: Document environment variable

DTSPEC_ROOT=os.path.join(os.getcwd(), 'dtspec')
if 'DTSPEC_ROOT' in os.environ:
    DTSPEC_ROOT=os.environ['DTSPEC_ROOT']
SCHEMAS_PATH = os.path.join(DTSPEC_ROOT, 'schemas')

class NothingToDoError(Exception): pass

def parse_args():
    parser = argparse.ArgumentParser(description='dtspec cli')
    parser.add_argument('--env', dest='env', default=None)

    subparsers = parser.add_subparsers(help='dtspec subcommand')

    db_parser = subparsers.add_parser('db', help='db and schema operations')
    db_parser.set_defaults(subcommand='db')
    db_parser.add_argument('--fetch-schemas', dest='fetch_schemas', const=True, nargs='?', default=False)
    db_parser.add_argument('--init-test-db', dest='init_test_db', const=True, nargs='?', default=False)
    db_parser.add_argument('--clean', dest='clean', const=True, nargs='?', default=False)

    dbt_parser = subparsers.add_parser('test-dbt', help='run dtspec tests for dbt')
    dbt_parser.set_defaults(subcommand='test-dbt')
    dbt_parser.add_argument('--compile-only', dest='compile_only', const=True, nargs='?', default=False)
    dbt_parser.add_argument('--models', dest='models', default=None)
    dbt_parser.add_argument('--skip-seed', dest='skip_seed', const=True, nargs='?', default=False)
    dbt_parser.add_argument('--partial-parse', dest='partial_parse', const=True, nargs='?', default=False)
    dbt_parser.add_argument('--target', dest='target', default='dtspec')

    dbt_parser.add_argument('--scenarios', dest='scenarios', default=None)
    dbt_parser.add_argument('--cases', dest='cases', default=None)

    return parser.parse_args()

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
    args = parse_args()
    config = get_config()

    if args.subcommand == 'db':
        return main_db(args, config)

    if args.subcommand == 'test-dbt':
        return main_test_dbt(args, config)

    raise NothingToDoError

def main_db(args, config):
    if args.fetch_schemas:
        fetch_schemas(config, args.env)
        return

    if args.init_test_db:
        init_test_db(config, args.env, args.clean)
        return

    raise NothingToDoError

def main_test_dbt(args, config):
    if not args.partial_parse:
        dtspec.shell.run_dbt('compile', target=args.target)

    # TODOC: Assumes we're running this in the dbt directory
    with open('target/manifest.json') as mfile:
        dbt_manifest = json.loads(mfile.read())
    manifest = dtspec.specs.compile_dbt_manifest(dbt_manifest)

    compiled_specs = compile_dtspec(scenario_selector=args.scenarios, case_selector=args.cases, manifest=manifest)
    if args.compile_only:
        return

    api = dtspec.api.Api(compiled_specs)
    api.generate_sources()

    if not args.skip_seed:
        dtspec.shell.run_dbt(
            'seed',
            target=args.target,
        )

    source_engines = {
        env: _engine_from_config(env_val['test'])
        for env, env_val in config['source_environments'].items()
    }
    _clean_target_test_data(config, api, target=args.target)
    _load_test_data(source_engines, api)

    dtspec.shell.run_dbt(
        'run',
        target=args.target,
        models=args.models,
        full_refresh=True, # Not yet supporting tests for incremental loads
        partial_parse=True, # The compile step above already parsed dbt
    )

    api.load_actuals(
        _get_actuals(config, api, target=args.target)
    )
    api.assert_expectations()


def _clean_target_test_data(config, api, target):
    target_config = config['target_environments'][target]
    engine = _engine_from_config(target_config)
    LOG.info(f'Cleaning out target test data for target test environment {target}')
    dtspec.db.clean_target_test_data(engine, api)

def _load_test_data(source_engines, api):
    dtspec.db.load_test_data(
        source_engines=source_engines,
        api=api,
        schemas_path=SCHEMAS_PATH,
    )

def _get_actuals(config, api, target):
    target_config = config['target_environments'][target]
    engine = _engine_from_config(target_config)
    LOG.info(f'Fetching results of run from target test environment {target}')
    return dtspec.db.get_actuals(engine, api)



def _engine_from_config(schema_config):
    return dtspec.db.generate_engine(
        engine_type=schema_config['type'],
        host=schema_config['host'],
        port=schema_config.get('port'),
        user=schema_config['user'],
        password=schema_config['password'],
        dbname=schema_config['dbname'],
        warehouse=schema_config.get('warehouse'),
        role=schema_config.get('role'),
    )


def _fetch_schema(config, env):
    LOG.info('fetching schemas for env: %s', env)

    env_config = config['source_environments'][env]
    engine = _engine_from_config(env_config['schema'])

    output_path = os.path.join(DTSPEC_ROOT, 'schemas')
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    for namespace, tables in env_config['tables'].items():
        dtspec.db.reflect(
            env=env,
            engine=engine,
            output_path=output_path,
            namespace=namespace,
            tables=tables,
        )

def fetch_schemas(config, env=None):
    envs = [env] if env else list(config['source_environments'].keys())
    for env in envs:
        _fetch_schema(config, env)


def _init_test_db(config, env=None, clean=False):
    LOG.info('initializing test db env: %s', env)
    env_config = config['source_environments'][env]
    engine = _engine_from_config(env_config['test'])

    dtspec.db.init_test_db(
        env=env,
        engine=engine,
        schemas_path=SCHEMAS_PATH,
        clean=clean
    )

def init_test_db(config, env=None, clean=False):
    envs = [env] if env else list(config['source_environments'].keys())
    for env in envs:
        _init_test_db(config, env, clean=clean)



# TODO: a dtspec init command that sets up a blank/example specs/main.yml file
# TODO: Compile dbt refs
# Also, need to deal with the fact that there could be multiple source scheams (PROD_RAW/PROD_SNAPSHOTS)

def compile_dtspec(scenario_selector=None, case_selector=None, manifest=None):
    search_path = os.path.join(DTSPEC_ROOT, 'specs')
    compiled_spec = dtspec.specs.compile_spec(search_path, scenario_selector=scenario_selector, case_selector=case_selector, manifest=manifest)

    with open(os.path.join(DTSPEC_ROOT, 'compiled_specs.yml'), 'w') as compiled_file:
        compiled_file.write(
            yaml.dump(compiled_spec, default_flow_style=False)
        )

    return compiled_spec

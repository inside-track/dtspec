import os
import json
import argparse
import pathlib
import shutil

import yaml
import jinja2
import dtspec

import dtspec.db
import dtspec.specs
import dtspec.shell

from dtspec.log import LOG

DTSPEC_ROOT = os.path.join(os.getcwd(), "dtspec")
DBT_ROOT = os.getcwd()
if "DTSPEC_ROOT" in os.environ:
    DTSPEC_ROOT = os.environ["DTSPEC_ROOT"]
if "DBT_ROOT" in os.environ:
    DBT_ROOT = os.environ["DBT_ROOT"]
SCHEMAS_PATH = os.path.join(DTSPEC_ROOT, "schemas")


class NothingToDoError(Exception):
    pass


class InvalidConfigFile(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(description="dtspec cli")

    subparsers = parser.add_subparsers(help="dtspec subcommand")

    init_parser = subparsers.add_parser("init", help="initialize a new dtspec project")
    init_parser.set_defaults(subcommand="init")
    init_parser.add_argument(
        "--name",
        dest="name",
        default="dtspec",
        help="name of dtspec path (default: dtspec; recommend run this command in the dbt directory)",
    )

    db_parser = subparsers.add_parser("db", help="db and schema operations")
    db_parser.set_defaults(subcommand="db")
    db_parser.add_argument(
        "--env",
        dest="env",
        default=None,
        help="use to specify source environment for schema commands (default: all)",
    )

    db_parser.add_argument(
        "--fetch-schemas",
        dest="fetch_schemas",
        const=True,
        nargs="?",
        default=False,
        help="reflect table schemas from source schema databases (results saved to schemas directory)",
    )
    db_parser.add_argument(
        "--init-test-db",
        dest="init_test_db",
        const=True,
        nargs="?",
        default=False,
        help="initialize test database using reflected table schemas (creates empty tables)",
    )
    db_parser.add_argument(
        "--clean",
        dest="clean",
        const=True,
        nargs="?",
        default=False,
        help="removes all tables in test database (can be combined with --init-test-db)",
    )

    dbt_parser = subparsers.add_parser("test-dbt", help="run dtspec tests for dbt")
    dbt_parser.set_defaults(subcommand="test-dbt")
    dbt_parser.add_argument(
        "--compile-only",
        dest="compile_only",
        const=True,
        nargs="?",
        default=False,
        help="skips running tests, only compiles dtspec spec files (results saved in compiled_specs.yml",
    )
    dbt_parser.add_argument("--models", dest="models", default=None)
    dbt_parser.add_argument(
        "--skip-seed",
        dest="skip_seed",
        const=True,
        nargs="?",
        default=False,
        help="skip running `dbt seed` before `dbt run`",
    )
    dbt_parser.add_argument(
        "--partial-parse",
        dest="partial_parse",
        const=True,
        nargs="?",
        default=False,
        help="skip re-compiling the dbt project (which only needs to be if the dbt code changed from the last dtspec run)",
    )
    dbt_parser.add_argument(
        "--target",
        dest="target",
        default="dtspec",
        help="specify the name of the dbt target (default: dtspec)",
    )

    dbt_parser.add_argument(
        "--scenarios",
        dest="scenarios",
        default=None,
        help="a regular expression that selects matching scenarios names",
    )
    dbt_parser.add_argument(
        "--cases",
        dest="cases",
        default=None,
        help="a regular expression that selects matching case names",
    )

    return parser.parse_args()


def get_config():
    "Read and parse configuration file (via Jinja2)"

    template_loader = jinja2.FileSystemLoader(searchpath=DTSPEC_ROOT)
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template("config.yml")
    rendered_template = template.render(
        env_var=lambda var, default="": os.environ.get(var, default)
    )
    config = yaml.safe_load(rendered_template)

    _validate_config(config)
    return config


def _validate_config(config):
    for env_name, source_env in config["source_environments"].items():
        schema_config = (
            source_env["schema"].get("host"),
            source_env["schema"].get("account"),
            source_env["schema"].get("dbname"),
            source_env["schema"].get("database"),
        )
        test_config = (
            source_env["test"].get("host"),
            source_env["test"].get("account"),
            source_env["test"].get("dbname"),
            source_env["test"].get("database"),
        )
        if schema_config == test_config:
            raise InvalidConfigFile(
                f"schema and test environments are the same for environment `{env_name}`\n"
                "these environments need to be different to prevent test data overwriting production!"
            )


def main():
    args = parse_args()
    config = get_config()

    if args.subcommand == "init":
        return main_init(args)

    if args.subcommand == "db":
        return main_db(args, config)

    if args.subcommand == "test-dbt":
        return main_test_dbt(args, config)

    raise NothingToDoError


def main_init(args):
    template_path = os.path.join(pathlib.Path(__file__).parent.absolute(), "init")
    target_path = os.path.join(os.getcwd(), args.name)
    shutil.copytree(template_path, target_path)


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
        dtspec.shell.run_dbt("compile", target=args.target)

    with open(
        os.path.join(DBT_ROOT, "target", "manifest.json"), encoding="utf-8"
    ) as mfile:
        dbt_manifest = json.loads(mfile.read())
    manifest = dtspec.specs.compile_dbt_manifest(dbt_manifest)

    compiled_specs = compile_dtspec(
        scenario_selector=args.scenarios, case_selector=args.cases, manifest=manifest
    )
    if args.compile_only:
        return

    api = dtspec.api.Api(compiled_specs)
    api.generate_sources()

    if not args.skip_seed:
        dtspec.shell.run_dbt(
            "seed",
            target=args.target,
        )

    source_engines = {
        env: _engine_from_config(env_val["test"])
        for env, env_val in config["source_environments"].items()
    }
    _clean_target_test_data(config, api, target=args.target)
    _load_test_data(source_engines, api)

    dtspec.shell.run_dbt(
        "run",
        target=args.target,
        models=args.models,
        full_refresh=True,  # Not yet supporting tests for incremental loads
        partial_parse=True,  # The compile step above already parsed dbt
    )

    api.load_actuals(_get_actuals(config, api, target=args.target))
    api.assert_expectations()


def _clean_target_test_data(config, api, target):
    target_config = config["target_environments"][target]
    engine = _engine_from_config(target_config)
    LOG.info("Cleaning out target test data for target test environment %s", target)
    dtspec.db.clean_target_test_data(engine, api)


def _load_test_data(source_engines, api):
    dtspec.db.load_test_data(
        source_engines=source_engines,
        api=api,
        schemas_path=SCHEMAS_PATH,
    )


def _get_actuals(config, api, target):
    target_config = config["target_environments"][target]
    engine = _engine_from_config(target_config)
    LOG.info("Fetching results of run from target test environment %s", target)
    return dtspec.db.get_actuals(engine, api)


def _engine_from_config(schema_config):
    return dtspec.db.generate_engine(**schema_config)


def _fetch_schema(config, env):
    LOG.info("fetching schemas for env: %s", env)

    env_config = config["source_environments"][env]
    engine = _engine_from_config(env_config["schema"])

    output_path = os.path.join(DTSPEC_ROOT, "schemas")
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    for namespace, tables in env_config["tables"].items():
        dtspec.db.reflect(
            env=env,
            engine=engine,
            output_path=output_path,
            namespace=namespace,
            tables=tables,
        )


def fetch_schemas(config, env=None):
    envs = [env] if env else list(config["source_environments"].keys())
    for this_env in envs:
        _fetch_schema(config, this_env)


def _init_test_db(config, env=None, clean=False):
    LOG.info("initializing test db env: %s", env)
    env_config = config["source_environments"][env]
    engine = _engine_from_config(env_config["test"])

    dtspec.db.init_test_db(
        env=env, engine=engine, schemas_path=SCHEMAS_PATH, clean=clean
    )


def init_test_db(config, env=None, clean=False):
    envs = [env] if env else list(config["source_environments"].keys())
    for this_env in envs:
        _init_test_db(config, this_env, clean=clean)


def compile_dtspec(scenario_selector=None, case_selector=None, manifest=None):
    search_path = os.path.join(DTSPEC_ROOT, "specs")
    compiled_spec = dtspec.specs.compile_spec(
        search_path,
        scenario_selector=scenario_selector,
        case_selector=case_selector,
        manifest=manifest,
    )

    with open(
        os.path.join(DTSPEC_ROOT, "compiled_specs.yml"), "w", encoding="utf-8"
    ) as compiled_file:
        compiled_file.write(yaml.dump(compiled_spec, default_flow_style=False))

    return compiled_spec

import os
import re
import datetime

import jinja2
import yaml
from dateutil.relativedelta import relativedelta


def str_presenter(dumper, data):
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(str, str_presenter)


def compile_spec(
    search_path, scenario_selector=None, case_selector=None, manifest=None
):
    scenario_selector = re.compile(scenario_selector or r".*")
    case_selector = re.compile(case_selector or r".*")

    template_loader = jinja2.FileSystemLoader(searchpath=search_path)
    template_env = jinja2.Environment(loader=template_loader)

    spec = _render_template(template_env, "main.yml", manifest=manifest)

    for yaml_file in _collect_files(search_path):
        file_spec = _render_template(template_env, yaml_file, manifest=manifest)
        for extendable in [
            "sources",
            "targets",
            "factories",
            "scenarios",
            "identifiers",
        ]:
            spec[extendable].extend(file_spec.pop(extendable, []))

    selected_scenario_cases = list(
        filter(
            lambda scenario: re.compile(scenario_selector).search(scenario["scenario"]),
            spec["scenarios"],
        )
    )

    for scenario in selected_scenario_cases:
        scenario["cases"] = list(
            filter(
                lambda case: re.compile(case_selector).search(case["case"]),
                scenario["cases"],
            )
        )

    spec["scenarios"] = selected_scenario_cases

    selected_targets = set()
    for scenario in spec["scenarios"]:
        for case in scenario["cases"]:
            for expectation in case["expected"]["data"]:
                selected_targets |= {expectation["target"]}

    spec["targets"] = [
        target for target in spec["targets"] if target["target"] in selected_targets
    ]

    return spec


def _collect_files(search_path):
    spec_templates = []
    for dirpath, _dirnames, filenames in os.walk(search_path):
        for filename in filenames:
            if not filename.endswith(".yml") or filename in "main.yml":
                continue

            rel_filename = os.path.relpath(os.path.join(dirpath, filename), search_path)
            spec_templates.append(rel_filename)
    return spec_templates


def dbt_source(manifest, source_name, name):
    source = manifest[(source_name, name)]
    return f"{source['database']}.{source['schema']}.{source['name']}"


def dbt_ref(manifest, name):
    ref = manifest[name]
    return f"{ref['database']}.{ref['schema']}.{ref['name']}"


def _render_template(template_env, yaml_file, manifest=None):
    manifest = manifest or {}

    template = template_env.get_template(yaml_file)
    rendered_template = template.render(
        datetime=datetime.datetime,
        date=datetime.date,
        relativedelta=relativedelta,
        UTCNOW=datetime.datetime.utcnow(),
        TODAY=datetime.date.today(),
        YESTERDAY=datetime.date.today() - relativedelta(days=1),
        TOMORROW=datetime.date.today() + relativedelta(days=1),
        dbt_source=lambda source_name, name: dbt_source(manifest, source_name, name),
        dbt_ref=lambda name: dbt_ref(manifest, name),
    )
    return yaml.safe_load(rendered_template)


def compile_dbt_manifest(dbt_manifest):
    dbt_manifest = dbt_manifest or {}
    return {
        **{
            (value["source_name"], value["name"]): {
                "database": value["database"],
                "schema": value["schema"],
                "name": value["name"],
            }
            for _key, value in dbt_manifest["sources"].items()
        },
        **{
            value["name"]: {
                "database": value["database"],
                "schema": value["schema"],
                "name": value["alias"],
            }
            for _key, value in dbt_manifest["nodes"].items()
            if value["resource_type"] == "model"
        },
        **{
            value["name"]: {
                "database": value["database"],
                "schema": value["schema"],
                "name": value["alias"],
            }
            for _key, value in dbt_manifest["nodes"].items()
            if value["resource_type"] == "snapshot"
        },
    }

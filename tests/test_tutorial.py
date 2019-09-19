import json

import yaml
import pandas as pd

import pytest

import dts.api
from dts.core import markdown_to_df


def parse_sources(sources):
    "Converts test data returned from dts api into Pandas dataframes"

    return {
        source_name: pd.DataFrame.from_records(data.serialize())
        for source_name, data in sources.items()
    }


def serialize_actuals(actuals):
    "Converts Pandas dataframe results into form needed to load dts api actuals"

    return {
        target_name: json.loads(dataframe.to_json(orient="records"))
        for target_name, dataframe in actuals.items()
    }


def hello_word_transformer(raw_students):
    def salutation(row):
        if row["clique"] == "Scooby Gang":
            return "Hello {}".format(row["name"])
        return "Goodbye {}".format(row["name"])

    salutations_df = raw_students.copy()
    salutations_df["salutation"] = salutations_df.apply(salutation, axis=1)

    return {"salutations": salutations_df}


@pytest.fixture
def specs():
    all_specs = yaml.safe_load_all(open("tests/tutorial_spec.yml"))
    return {spec["description"].split("-")[0].strip(): spec for spec in all_specs}


# @pytest.fixture
# def hello_world_spec(spec):
# #    return next(filter(lambda v: v['description'].startswith('HelloWorld'), spec))


def test_hello_world_spec(specs):
    api = dts.api.Api(specs["HelloWorld"])
    api.generate_sources()

    sources_data = parse_sources(api.spec["sources"])
    actual_data = hello_word_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    api.run_assertions()


# def test_extra_case_failure(specs):
#     api = dts.api.Api(specs['ExtraCaseFailure'])
#     api.generate_sources()

#     sources_data = parse_sources(api.spec['sources'])
#     actual_data = hello_word_transformer(**sources_data)
#     serialized_actuals = serialize_actuals(actual_data)
#     api.load_actuals(serialized_actuals)

#     api.run_assertions()

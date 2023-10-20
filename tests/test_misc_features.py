import json

import yaml
import pandas as pd

import pytest

import dtspec.api

# pylint: disable=redefined-outer-name


def parse_sources(sources):
    "Converts test data returned from dtspec api into Pandas dataframes"

    return {
        source_name: pd.DataFrame.from_records(data.serialize())
        for source_name, data in sources.items()
    }


def serialize_actuals(actuals):
    "Converts Pandas dataframe results into form needed to load dtspec api actuals"

    def stringify_pd(df):
        nulls_df = df.map(lambda v: v is None)
        str_df = df.astype({column: str for column in df.columns})

        def replace_nulls(series1, series2):
            return series1.combine(
                series2, lambda value1, value2: value1 if not value2 else "{NULL}"
            )

        return str_df.combine(nulls_df, replace_nulls)

    return {
        target_name: {
            "records": json.loads(stringify_pd(dataframe).to_json(orient="records")),
            "columns": list(dataframe.columns),
        }
        for target_name, dataframe in actuals.items()
    }


def transformer(raw_students):
    df = raw_students.rename(columns={"id": "student_id", "external_id": "card_id"})

    df["name_is_null"] = df["first_name"].apply(lambda v: v is None)
    df = df[~df["card_id"].isnull()]

    return {"students_transformed": df}


@pytest.fixture
def spec():
    return yaml.safe_load(open("tests/misc_features.yml"))


@pytest.fixture
def api(spec):
    api = dtspec.api.Api(spec)
    api.generate_sources()
    return api


@pytest.fixture
def sources_data(api):
    return parse_sources(api.spec["sources"])


@pytest.fixture
def serialized_actuals(sources_data):
    actual_data = transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    return serialized_actuals


@pytest.fixture
def api_w_actuals(api, serialized_actuals):
    api.load_actuals(serialized_actuals)
    return api


def test_all_passing_exceptions(api_w_actuals):
    api_w_actuals.assert_expectations()

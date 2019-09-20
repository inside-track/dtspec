import json

import yaml
import pandas as pd

import pytest

import dts.api
from dts.core import markdown_to_df

from tests import assert_frame_equal

# pylint: disable=redefined-outer-name

# TODO: rename to test_api_features


def parse_sources(sources):
    "Converts test data returned from dts api into Pandas dataframes"

    return {
        source_name: pd.DataFrame.from_records(data.serialize())
        for source_name, data in sources.items()
    }


def serialize_actuals(actuals):
    "Converts Pandas dataframe results into form needed to load dts api actuals"

    return {
        target_name: json.loads(dataframe.astype(str).to_json(orient="records"))
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


def realistic_transformer(
    raw_students, raw_schools, raw_classes, dim_date, exclude_missing_classes=True
):
    if exclude_missing_classes:
        classes_how = "inner"
    else:
        classes_how = "left"

    student_schools = raw_students.rename(
        columns={"id": "student_id", "external_id": "card_id"}
    ).merge(
        raw_schools.rename(columns={"id": "school_id", "name": "school_name"}),
        how="inner",
        on="school_id",
    )

    student_classes = student_schools.merge(
        raw_classes.rename(columns={"name": "class_name"}),
        how=classes_how,
        on="student_id",
    ).merge(
        dim_date.rename(columns={"date": "start_date"}), how="left", on="start_date"
    )

    students_per_school = (
        student_schools.groupby(["school_name"])
        .size()
        .to_frame(name="number_of_students")
        .reset_index()
    )

    return {
        "student_classes": student_classes,
        "students_per_school": students_per_school,
    }


@pytest.fixture
def specs():
    all_specs = yaml.safe_load_all(open("tests/tutorial_spec.yml"))
    return {spec["description"].split("-")[0].strip(): spec for spec in all_specs}

@pytest.fixture
def api(specs):
    api = dts.api.Api(specs["Realistic"])
    api.generate_sources()
    return api

def test_source_data_stacks_for_every_case(api):
    sources_data = parse_sources(api.spec["sources"])

    actual = len(sources_data["raw_schools"])
    n_cases = sum([len(scenario.cases) for scenario in api.spec["scenarios"].values()])
    expected = n_cases * 2
    assert actual == expected

def test_stacked_source_data_has_unique_identifiers(api):
    sources_data = parse_sources(api.spec["sources"])

    student_ids =  sources_data["raw_students"]["id"]
    expected = len(student_ids)
    actual = len(set(student_ids))
    assert actual == expected

def test_source_without_identifer_not_stacked(api):
    sources_data = parse_sources(api.spec["sources"])

    actual = len(sources_data["dim_date"])
    expected = 4
    assert actual == expected


def test_actuals_are_loaded(api):
    sources_data = parse_sources(api.spec["sources"])

    sources_data = parse_sources(api.spec["sources"])
    actual_data = realistic_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    expected = markdown_to_df(
        """
        | card_id | name   | school_name | class_name        | season       |
        | -       | -      | -           | -                 | -            |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Fall 2001    |
        | stu2    | Willow | Sunnydale   |     Good Spells   | Spring 2002  |
        | stu3    | Bill   | San Dimas   |         Station   | Fall 2002    |
        | stu4    | Ted    | San Dimas   | Being Excellent   | Fall 2002    |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | stu2    | Willow | Sunnydale   |     Good Spells   | Summer 2002  |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | stu2    | Willow | Sunnydale   |     Good Spells   | Summer 2002  |
        | stu2    | Willow | Sunnydale   | Season 6 Spells   | Summer 2002  |
        | stu3    | Bill   | San Dimas   |         Station   | Summer 2002  |
        | stu4    | Ted    | San Dimas   | Being Excellent   | Summer 2002  |
        | stu4    | Ted    | San Dimas   |         Station   | Summer 2002  |
        """
    )

    actual = api.spec["targets"]["student_classes"].data[expected.columns]
    assert_frame_equal(actual, expected)


def test_passing_expectation(api):
    sources_data = parse_sources(api.spec["sources"])

    sources_data = parse_sources(api.spec["sources"])
    actual_data = realistic_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)


    api.spec["scenarios"]["DenormalizingStudentClasses"].cases[
        "MissingClasses"
    ].assert_expectations()


def test_failing_expectation(api):
    sources_data = parse_sources(api.spec["sources"])
    actual_data = realistic_transformer(**sources_data, exclude_missing_classes=False)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    with pytest.raises(AssertionError):
        api.spec["scenarios"]["DenormalizingStudentClasses"].cases[
            "MissingClasses"
        ].assert_expectations()




def test_hello_world_spec(specs):
    api = dts.api.Api(specs["HelloWorld"])
    api.generate_sources()

    sources_data = parse_sources(api.spec["sources"])
    actual_data = hello_word_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    api.run_assertions()


def test_multiple_cases(specs):
    api = dts.api.Api(specs["MultipleCases"])
    api.generate_sources()

    sources_data = parse_sources(api.spec["sources"])
    actual_data = hello_word_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    api.run_assertions()


def test_realistic(specs):
    api = dts.api.Api(specs["Realistic"])
    api.generate_sources()

    sources_data = parse_sources(api.spec["sources"])
    actual_data = realistic_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    api.run_assertions()

import json

import yaml
import pandas as pd
from colorama import Fore, Style

import pytest

import dtspec.api
from dtspec.core import markdown_to_df

from tests import assert_frame_equal

# pylint: disable=redefined-outer-name


def parse_sources(sources):
    "Converts test data returned from dtspec api into Pandas dataframes"

    return {
        source_name: pd.DataFrame.from_records(data.serialize())
        for source_name, data in sources.items()
    }


def serialize_actuals(actuals):
    "Converts Pandas dataframe results into form needed to load dtspec api actuals"

    return {
        target_name: {
            "records": json.loads(dataframe.astype(str).to_json(orient="records")),
            "columns": list(dataframe.columns),
        }
        for target_name, dataframe in actuals.items()
    }


def hello_world_transformer(raw_students):
    salutations_df = raw_students.copy()
    salutations_df["salutation"] = salutations_df["name"].apply(lambda v: "Hello " + v)

    return {"salutations": salutations_df}


def hello_world_multiple_transformer(raw_students):
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

    student_classes["student_class_id"] = student_classes.apply(
        lambda row: "-".join([str(row["card_id"]), str(row["class_name"])]), axis=1
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
def spec():
    return yaml.safe_load(open("tests/realistic.yml", encoding="utf-8"))


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
    actual_data = realistic_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    return serialized_actuals


@pytest.fixture
def api_w_actuals(api, serialized_actuals):
    api.load_actuals(serialized_actuals)
    return api


def test_source_data_stacks_for_every_case(api, sources_data):
    actual = len(sources_data["raw_schools"])
    n_cases = sum([len(scenario.cases) for scenario in api.spec["scenarios"].values()])
    expected = n_cases * 2
    assert actual == expected


def test_stacked_source_data_has_unique_identifiers(sources_data):
    student_ids = sources_data["raw_students"]["id"]
    expected = len(student_ids)
    actual = len(set(student_ids))
    assert actual == expected


def test_source_without_identifer_not_stacked(sources_data):
    actual = len(sources_data["dim_date"])
    expected = 4
    assert actual == expected


def test_actuals_are_loaded(api_w_actuals):
    api = api_w_actuals

    expected = markdown_to_df(
        """
        | card_id | name   | school_name | class_name        | season       |
        | -       | -      | -           | -                 | -            |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Fall 2001    | # BasicDenormalization
        | stu2    | Willow | Sunnydale   |     Good Spells   | Spring 2002  |
        | stu3    | Bill   | San Dimas   |         Station   | Fall 2002    |
        | stu4    | Ted    | San Dimas   | Being Excellent   | Fall 2002    |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Summer 2002  | # MissingClasses
        | stu2    | Willow | Sunnydale   |     Good Spells   | Summer 2002  |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | stu2    | Willow | Sunnydale   |     Good Spells   | Summer 2002  | # MultipleClasses
        | stu2    | Willow | Sunnydale   | Season 6 Spells   | Summer 2002  |
        | stu3    | Bill   | San Dimas   |         Station   | Summer 2002  |
        | stu4    | Ted    | San Dimas   | Being Excellent   | Summer 2002  |
        | stu4    | Ted    | San Dimas   |         Station   | Summer 2002  |
        | stu1    | Buffy  | Sunnydale   |  Applied Stabby   | Fall 2001    | # IdConcatenation
        | stu2    | Willow | Sunnydale   |     Good Spells   | Spring 2002  |
        | stu3    | Bill   | San Dimas   |         Station   | Fall 2002    |
        | stu4    | Ted    | San Dimas   | Being Excellent   | Fall 2002    |
        """
    )

    actual = api.spec["targets"]["student_classes"].data[expected.columns]
    assert_frame_equal(actual, expected)


def test_passing_expectation(api_w_actuals):
    api = api_w_actuals

    api.spec["scenarios"]["DenormalizingStudentClasses"].cases[
        "MissingClasses"
    ].assert_expectations()


def test_all_passing_exceptions(api_w_actuals):
    api_w_actuals.assert_expectations()


def test_failing_expectation(api, sources_data):
    actual_data = realistic_transformer(**sources_data, exclude_missing_classes=False)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    with pytest.raises(AssertionError):
        api.spec["scenarios"]["DenormalizingStudentClasses"].cases[
            "MissingClasses"
        ].assert_expectations()


def test_hello_world_spec():
    spec = yaml.safe_load(open("tests/hello_world.yml", encoding="utf-8"))
    api = dtspec.api.Api(spec)
    api.generate_sources()

    sources_data = parse_sources(api.spec["sources"])
    actual_data = hello_world_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    api.assert_expectations()


def test_hello_world_multiple_cases_spec():
    spec = yaml.safe_load(
        open("tests/hello_world_multiple_cases.yml", encoding="utf-8")
    )
    api = dtspec.api.Api(spec)
    api.generate_sources()

    sources_data = parse_sources(api.spec["sources"])
    actual_data = hello_world_multiple_transformer(**sources_data)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    api.assert_expectations()


def test_assertion_messages(api, sources_data, capsys):
    actual_data = realistic_transformer(**sources_data, exclude_missing_classes=False)
    serialized_actuals = serialize_actuals(actual_data)
    api.load_actuals(serialized_actuals)

    with pytest.raises(AssertionError):
        api.assert_expectations()

    captured = capsys.readouterr()

    assert (
        f"Asserting DenormalizingStudentClasses: BasicDenormalization {Fore.GREEN}PASSED{Style.RESET_ALL}"
        in captured.out
    )
    assert (
        f"Asserting DenormalizingStudentClasses: MissingClasses {Fore.RED}FAILED"
        in captured.out
    )


def test_markdown(api):
    actual = api.to_markdown()
    with open("tests/__actual_realistic.md", "w") as actual_output:
        actual_output.write(actual)
    with open("tests/recorded_realistic.md", "r") as recorded_output:
        expected = recorded_output.read()

    assert actual == expected

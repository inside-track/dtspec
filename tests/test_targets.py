import copy
import pytest

import dtspec.core
from dtspec.core import markdown_to_df, Identifier, Target, Case

from tests import assert_frame_equal

# pylint: disable=redefined-outer-name


@pytest.fixture
def identifiers():
    return {
        "student": Identifier(
            {"id": {"generator": "unique_integer"}, "uuid": {"generator": "uuid"}}
        )
    }


@pytest.fixture
def simple_target(identifiers):
    return Target(
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}}
    )


@pytest.fixture
def cases():
    return [Case(name="TestCase1"), Case(name="TestCase2")]


@pytest.fixture
def stu(identifiers, cases):
    return {
        "c1stu1": identifiers["student"].generate(case=cases[0], named_id="stu1")["id"],
        "c1stu2": identifiers["student"].generate(case=cases[0], named_id="stu2")["id"],
        "c2stu1": identifiers["student"].generate(case=cases[1], named_id="stu1")["id"],
        "c2stu2": identifiers["student"].generate(case=cases[1], named_id="stu2")["id"],
    }


@pytest.fixture
def simple_data(stu):
    return [
        {"id": stu["c1stu1"], "first_name": "Buffy"},
        {"id": stu["c1stu2"], "first_name": "Willow"},
        {"id": stu["c2stu1"], "first_name": "Faith"},
        {"id": stu["c2stu2"], "first_name": "Willow"},
    ]


def test_actual_data_is_loaded_ids_translated(simple_target, simple_data):
    simple_target.load_actual(simple_data)

    actual = simple_target.data.drop(columns="__dtspec_case__")
    expected = markdown_to_df(
        """
        | id   | first_name |
        | -    | -          |
        | stu1 | Buffy      |
        | stu2 | Willow     |
        | stu1 | Faith      |
        | stu2 | Willow     |
        """
    )

    assert_frame_equal(actual, expected)


def test_target_can_be_split_into_case(simple_target, simple_data, cases):
    simple_target.load_actual(simple_data)

    actual = simple_target.case_data(cases[1])
    expected = markdown_to_df(
        """
        | id   | first_name |
        | -    | -          |
        | stu1 | Faith      |
        | stu2 | Willow     |
        """
    )

    assert_frame_equal(actual, expected)


def test_raises_error_if_raw_id_not_found(simple_target, simple_data):
    bad_data = copy.deepcopy(simple_data)
    bad_data[1]["id"] = 123456789
    with pytest.raises(dtspec.core.UnableToFindNamedIdError):
        simple_target.load_actual(bad_data)


def test_empty_data_can_be_loaded_with_columns_specified(simple_target):
    simple_target.load_actual([], columns=["id", "first_name"])

    actual = simple_target.data.drop(columns="__dtspec_case__")
    expected = markdown_to_df(
        """
        | id   | first_name |
        | -    | -          |
        """
    )

    assert_frame_equal(actual, expected)


def test_raises_if_data_is_empty_wo_columns_specified(simple_target):
    with pytest.raises(dtspec.core.EmptyDataNoColumnsError):
        simple_target.load_actual([])

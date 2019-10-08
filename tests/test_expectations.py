import pandas as pd

import pytest

from dtspec.core import markdown_to_df, Target
from dtspec.expectations import DataExpectation, MissingExpectedKeysAssertionError


# pylint: disable=redefined-outer-name


@pytest.fixture
def expected_table():
    return """
    | id | name   |
    | -  | -      |
    | 1  | Buffy  |
    | 2  | Willow |
    | 3  | Xander |
    """


@pytest.fixture
def target():
    return Target(name="some_target")


@pytest.fixture
def actual_data(expected_table):
    return markdown_to_df(expected_table)


def test_passes_when_data_is_the_same(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table)
    expectation.load_actual(actual_data.copy())
    expectation.assert_expected()


def test_fails_when_data_is_different(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table)

    actual_data = actual_data.copy()
    actual_data["name"].iloc[1] = "Evil Willow"
    expectation.load_actual(actual_data)

    with pytest.raises(AssertionError):
        expectation.assert_expected()


def test_fails_if_sorted_differently(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table)

    actual_data = actual_data.copy().sort_values("id", ascending=False)
    expectation.load_actual(actual_data)

    with pytest.raises(AssertionError):
        expectation.assert_expected()


def test_passes_if_sorted_differently_using_by(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table, by=["id"])

    actual_data = actual_data.copy().sort_values("id", ascending=False)
    expectation.load_actual(actual_data)

    expectation.assert_expected()


def test_extra_columns_in_actual_are_ignored(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table)
    actual_data = actual_data.copy()
    actual_data["eman"] = actual_data["name"].apply(lambda v: v[::-1])
    expectation.load_actual(actual_data)
    expectation.assert_expected()


def test_raise_on_missing_expected_column(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table)
    actual_data = actual_data.rename(columns={"name": "first_name"})
    expectation.load_actual(actual_data)
    with pytest.raises(AssertionError):
        expectation.assert_expected()


def test_raise_when_there_are_extra_actual_records(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table, by=["id"])
    actual_data = pd.concat(
        [actual_data, pd.DataFrame({"id": ["4"], "name": ["Dawn"]})]
    )
    expectation.load_actual(actual_data)
    with pytest.raises(AssertionError):
        expectation.assert_expected()


def test_passes_when_using_compare_on_keys(expected_table, actual_data, target):
    expectation = DataExpectation(target, expected_table, by=["id"], compare_via="keys")
    actual_data = pd.concat(
        [pd.DataFrame({"id": ["0"], "name": ["The First"]}), actual_data]
    )
    expectation.load_actual(actual_data)
    expectation.assert_expected()


def test_incompatible_keys_raise_specific_exception(target):
    expected_table = """
        | id | name   |
        | -  | -      |
        | 1  | Buffy  |
        | 2  | Willow |
        | 3  | Xander |
        """

    actual_data = markdown_to_df(
        """
        | id  | name      |
        | -   | -         |
        | 0.0 | The First |
        | 1.0 | Buffy     |
        | 2.0 | Willow    |
        | 3.0 | Xander    |
        """
    )

    expectation = DataExpectation(target, expected_table, by=["id"], compare_via="keys")
    expectation.load_actual(actual_data)
    with pytest.raises(MissingExpectedKeysAssertionError):
        expectation.assert_expected()


def test_setting_constant_values(expected_table, target):
    expectation = DataExpectation(
        target, expected_table, values={"school_name": "Sunnydale High"}
    )
    actual_data = markdown_to_df(expected_table)
    actual_data["school_name"] = "Sunnydale High"
    expectation.load_actual(actual_data.copy())
    expectation.assert_expected()

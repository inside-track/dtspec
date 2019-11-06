import pandas as pd

import pytest

from dtspec.core import markdown_to_df, Target, BadMarkdownTableError, Case, Identifier
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
def identifiers():
    return {
        "student": Identifier(
            {"id": {"generator": "unique_integer"}, "uuid": {"generator": "uuid"}}
        )
    }


@pytest.fixture
def target():
    return Target(name="some_target")


@pytest.fixture
def case():
    return Case(name="TestCase1")


@pytest.fixture
def actual_data(expected_table):
    return markdown_to_df(expected_table)


def test_passes_when_data_is_the_same(expected_table, actual_data, target, case):
    expectation = DataExpectation(target, expected_table)
    expectation.load_actual(actual_data.copy())
    expectation.assert_expected(case)


def test_fails_when_data_is_different(expected_table, actual_data, target, case):
    expectation = DataExpectation(target, expected_table)

    actual_data = actual_data.copy()
    actual_data["name"].iloc[1] = "Evil Willow"
    expectation.load_actual(actual_data)

    with pytest.raises(AssertionError):
        expectation.assert_expected(case)


def test_fails_if_sorted_differently(expected_table, actual_data, target, case):
    expectation = DataExpectation(target, expected_table)

    actual_data = actual_data.copy().sort_values("id", ascending=False)
    expectation.load_actual(actual_data)

    with pytest.raises(AssertionError):
        expectation.assert_expected(case)


def test_passes_if_sorted_differently_using_by(
    expected_table, actual_data, target, case
):
    expectation = DataExpectation(target, expected_table, by=["id"])

    actual_data = actual_data.copy().sort_values("id", ascending=False)
    expectation.load_actual(actual_data)

    expectation.assert_expected(case)


def test_extra_columns_in_actual_are_ignored(expected_table, actual_data, target, case):
    expectation = DataExpectation(target, expected_table)
    actual_data = actual_data.copy()
    actual_data["eman"] = actual_data["name"].apply(lambda v: v[::-1])
    expectation.load_actual(actual_data)
    expectation.assert_expected(case)


def test_raise_on_missing_expected_column(expected_table, actual_data, target, case):
    expectation = DataExpectation(target, expected_table)
    actual_data = actual_data.rename(columns={"name": "first_name"})
    expectation.load_actual(actual_data)
    with pytest.raises(AssertionError):
        expectation.assert_expected(case)


def test_raise_when_there_are_extra_actual_records(
    expected_table, actual_data, target, case
):
    expectation = DataExpectation(target, expected_table, by=["id"])
    actual_data = pd.concat(
        [actual_data, pd.DataFrame({"id": ["4"], "name": ["Dawn"]})]
    )
    expectation.load_actual(actual_data)
    with pytest.raises(AssertionError):
        expectation.assert_expected(case)


def test_passes_when_using_compare_on_keys(expected_table, actual_data, target, case):
    expectation = DataExpectation(target, expected_table, by=["id"], compare_via="keys")
    actual_data = pd.concat(
        [pd.DataFrame({"id": ["0"], "name": ["The First"]}), actual_data]
    )
    expectation.load_actual(actual_data)
    expectation.assert_expected(case)


def test_incompatible_keys_raise_specific_exception(target, case):
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
        expectation.assert_expected(case)


def test_setting_constant_values(expected_table, target, case):
    expectation = DataExpectation(
        target, expected_table, values={"school_name": "Sunnydale High"}
    )
    actual_data = markdown_to_df(expected_table)
    actual_data["school_name"] = "Sunnydale High"
    expectation.load_actual(actual_data.copy())
    expectation.assert_expected(case)


def test_raises_when_markdown_is_missing(target):
    with pytest.raises(BadMarkdownTableError):
        DataExpectation(target, None)


def test_raises_when_markdown_is_missing_header_sep(target):
    with pytest.raises(BadMarkdownTableError):
        DataExpectation(
            target,
            """
            | id | name   |
            | 1  | Buffy  |
            | 2  | Willow |
            | 3  | Xander |
            """,
        )


def test_raises_when_markdown_causes_pandas_failures(target):
    with pytest.raises(BadMarkdownTableError):
        DataExpectation(
            target,
            """
            | id name   |
            | - | - |
            | 1  |
            | 2  | Willow |
            | 3  | Xander |
            """,
        )


def test_embedded_identifiers_are_translated(target, case, identifiers):
    expected_table = """
        | prefixed_id           | name   |
        | -                     | -      |
        | SDU-{student.id[s1]}  | Buffy  |
        | SDU-{student.id[s2]}  | Willow |
        | SDU-{student.id[s3]}  | Xander |
    """

    expectation = DataExpectation(target, expected_table, identifiers=identifiers)

    actual_data = markdown_to_df(
        """
        | prefixed_id | name   |
        | -           | -      |
        | SDU-{s1}    | Buffy  |
        | SDU-{s2}    | Willow |
        | SDU-{s3}    | Xander |
        """.format(
            s1=identifiers["student"].generate(case=case, named_id="s1")["id"],
            s2=identifiers["student"].generate(case=case, named_id="s2")["id"],
            s3=identifiers["student"].generate(case=case, named_id="s3")["id"],
        )
    )

    expectation.load_actual(actual_data.copy())
    expectation.assert_expected(case)

import pandas as pd
import pytest

from dts.core import markdown_to_df
from dts.expectations import DataExpectation

@pytest.fixture
def sample_table():
    return '''
    | id | name   |
    | -  | -      |
    | 1  | Buffy  |
    | 2  | Willow |
    | 3  | Xander |
    '''

@pytest.fixture
def sample_data(sample_table):
    return markdown_to_df(sample_table)

def test_passes_when_data_is_the_same(sample_table, sample_data):
    expectation = DataExpectation('some_target', sample_table)
    expectation.load_actual(sample_data.copy())
    expectation.assert_expected()

def test_fails_when_data_is_different(sample_table, sample_data):
    expectation = DataExpectation('some_target', sample_table)

    actual_data = sample_data.copy()
    actual_data['name'].iloc[1] = 'Evil Willow'
    expectation.load_actual(actual_data)

    with pytest.raises(AssertionError):
        expectation.assert_expected()

def test_fails_if_sorted_differently(sample_table, sample_data):
    expectation = DataExpectation('some_target', sample_table)

    actual_data = sample_data.copy().sort_values('id', ascending=False)
    expectation.load_actual(actual_data)

    with pytest.raises(AssertionError):
        expectation.assert_expected()

def test_passes_if_sorted_differently_using_by(sample_table, sample_data):
    expectation = DataExpectation('some_target', sample_table, by=['id'])

    actual_data = sample_data.copy().sort_values('id', ascending=False)
    expectation.load_actual(actual_data)

    expectation.assert_expected()

def test_extra_columns_in_actual_are_ignored(sample_table, sample_data):
    expectation = DataExpectation('some_target', sample_table)
    actual_data = sample_data.copy()
    actual_data['eman'] = actual_data['name'].apply(lambda v: v[::-1])
    expectation.load_actual(actual_data)
    expectation.assert_expected()

def test_raise_on_missing_expected_column(sample_table, sample_data):
    expectation = DataExpectation('some_target', sample_table)
    actual_data = sample_data.rename(columns={'name': 'first_name'})
    expectation.load_actual(actual_data)
    with pytest.raises(AssertionError):
        expectation.assert_expected()

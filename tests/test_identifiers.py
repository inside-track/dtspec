import re
import pytest

import data_test_studio as dts


@pytest.fixture
def student():
    return dts.identifiers.Identifier({
        'id': {'generator': 'unique_integer'},
        'uuid': {'generator': 'uuid'},
        'external_id': {'generator': 'unique_string'}
    })


def test_unique_int_generates_int(student):
    assert isinstance(student['case']['name'].values['id'], int)

def test_unique_str_generates_str(student):
    assert isinstance(student['case']['name'].values['external_id'], str)

def test_uuid_generates_uuid(student):
    assert re.match(
        r'[0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12}',
        str(student['case']['name'].values['uuid'])
    )

def test_generated_values_are_memoized(student):
    initial_value = student['case']['name'].values['id']
    recalled_value = student['case']['name'].values['id']
    assert recalled_value == initial_value

def test_new_names_get_diff_values(student):
    some_name = student['case']['some_name'].values['id']
    new_name = student['case']['new_name'].values['id']
    assert some_name != new_name

def test_new_cases_get_diff_values(student):
    first_case = student['first_case']['name'].values['id']
    second_case = student['second_case']['name'].values['id']
    assert second_case != first_case

def test_unique_values_not_repeated_within_case(student):
    nvalues = 1000
    values = {student['case'][name].values['id'] for name in range(nvalues)}
    assert len(values) == nvalues

def test_unique_values_not_repeated_across_cases(student):
    nvalues = 1000
    values = {student[case]['name'].values['id'] for case in range(nvalues)}
    assert len(values) == nvalues

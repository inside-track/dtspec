import re
import pytest

import data_test_studio as dts


@pytest.fixture
def student():
    return dts.identifiers.Identifier({
        'id': {'generator': 'unique_integer'},
        'uuid': {'generator': 'uuid'},
        'external_id': {'generator': 'unique_string', 'prefix': 'TestPrefix-'}
    })


def test_unique_int_generates_int(student):
    assert isinstance(student.record(case='TestCase', named_id='stuX')['id'], int)

def test_unique_str_generates_str(student):
    assert isinstance(student.record(case='TestCase', named_id='stuX')['external_id'], str)

def test_uuid_generates_uuid(student):
    assert re.match(
        r'[0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12}',
        str(student.record(case='TestCase', named_id='stuX')['uuid'])
    )

def test_generated_records_are_memoized(student):
    initial_value = student.record(case='TestCase', named_id='stuX')['id']
    recalled_value = student.record(case='TestCase', named_id='stuX')['id']
    assert recalled_value == initial_value

def test_new_named_ids_get_diff_values(student):
    some_name = student.record(case='TestCase', named_id='stu1')['id']
    new_name = student.record(case='TestCase', named_id='stu2')['id']
    assert some_name != new_name

def test_new_cases_get_diff_values(student):
    first_case = student.record(case='TestCase1', named_id='stuX')['id']
    second_case = student.record(case='TestCase2', named_id='stuX')['id']
    assert second_case != first_case

def test_unique_values_not_repeated_within_case(student):
    nvalues = 1000
    values = {student.record(case='TestCase', named_id=name)['id'] for name in range(nvalues)}
    assert len(values) == nvalues

def test_unique_values_not_repeated_across_cases(student):
    nvalues = 1000
    values = {student.record(case=case, named_id='stuX')['id'] for case in range(nvalues)}
    assert len(values) == nvalues

def test_generators_can_be_passed_args(student):
    assert student.record(case='TestCase', named_id='stuX')['external_id'].startswith('TestPrefix-')

def test_generators_fail_when_passed_bad_args():
    with pytest.raises(TypeError):
        dts.identifiers.Identifier({
            'external_id': {'generator': 'unique_string', 'bad_arg': 'TestPrefix-'}
        })

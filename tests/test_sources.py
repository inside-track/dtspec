import json

import pytest
from pandas.util.testing import assert_frame_equal

import dts.identifiers
from dts.sources import Source

@pytest.fixture
def identifiers():
    return {
        'student': dts.identifiers.Identifier({
            'id': {'generator': 'unique_integer'}
        })
    }

@pytest.fixture
def simple_source(identifiers):
    return Source(
        id_mapping={
            'id': {
                'identifier': identifiers['student'],
                'attribute': 'id'
            }
        }
    )

def test_identifers_are_translated(simple_source, identifiers):
    simple_source.stack(
        'TestCase',
        '''
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        '''
    )

    actual = simple_source.data
    expected = dts.data.markdown_to_df(
        '''
        | id   | first_name |
        | -    | -          |
        | {s1} | Bob        |
        | {s2} | Nancy      |
        '''.format(
            s1=identifiers['student'].record(case='TestCase', named_id='s1')['id'],
            s2=identifiers['student'].record(case='TestCase', named_id='s2')['id'],
        )
    )
    assert_frame_equal(actual, expected)

def test_sources_stack(simple_source, identifiers):
    simple_source.stack(
        'TestCase1',
        '''
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        '''
    )

    simple_source.stack(
        'TestCase2',
        '''
        | id | first_name |
        | -  | -          |
        | s1 | Bobob      |
        | s2 | Nanci      |
        '''
    )


    actual = simple_source.data
    expected = dts.data.markdown_to_df(
        '''
        | id    | first_name |
        | -     | -          |
        | {s11} | Bob        |
        | {s12} | Nancy      |
        | {s21} | Bobob      |
        | {s22} | Nanci      |
        '''.format(
            s11=identifiers['student'].record(case='TestCase1', named_id='s1')['id'],
            s12=identifiers['student'].record(case='TestCase1', named_id='s2')['id'],
            s21=identifiers['student'].record(case='TestCase2', named_id='s1')['id'],
            s22=identifiers['student'].record(case='TestCase2', named_id='s2')['id'],
        )
    )
    assert_frame_equal(actual, expected)

def test_data_converts_to_json(simple_source, identifiers):
    simple_source.stack(
        'TestCase',
        '''
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        '''
    )

    actual = simple_source.to_json()
    expected = json.dumps([
        {
            'id': identifiers['student'].record(case='TestCase', named_id='s1')['id'],
            'first_name': 'Bob'
        },
        {
            'id': identifiers['student'].record(case='TestCase', named_id='s2')['id'],
            'first_name': 'Nancy'
        },
    ], separators=(',', ':'))

    assert actual == expected

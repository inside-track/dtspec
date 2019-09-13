from copy import deepcopy
import pytest

import dts
from dts.core import (
    markdown_to_df,
    Identifier,
    Factory,
    Source
)

from tests import assert_frame_equal

@pytest.fixture
def identifiers():
    return {
        'student': Identifier({
            'id': {'generator': 'unique_integer'},
            'external_id': {'generator': 'unique_string'},
        }),
        'organization': Identifier({
            'id': {'generator': 'unique_integer'},
            'uuid': {'generator': 'uuid'},
        })
    }

@pytest.fixture
def sources(identifiers):
    return {
        'students': Source(
            id_mapping={
                'id': {
                    'identifier': identifiers['student'],
                    'attribute': 'id'
                },
                'external_id': {
                    'identifier': identifiers['student'],
                    'attribute': 'external_id'
                },
                'organization_id': {
                    'identifier': identifiers['organization'],
                    'attribute': 'id'
                }
            },
            defaults={
                'external_id': {
                    'identifier': identifiers['student'],
                    'attribute': 'external_id'
                },
                'organization_id': {
                    'identifier': identifiers['organization'],
                    'attribute': 'id'
                },
            }
        ),
        'organizations': Source(
            id_mapping={
                'id': {
                    'identifier': identifiers['organization'],
                    'attribute': 'id'
                },
                'uuid': {
                    'identifier': identifiers['organization'],
                    'attribute': 'uuid'
                }
            },
            defaults={
                'uuid': {
                    'identifier': identifiers['organization'],
                    'attribute': 'uuid'
                }
            }
        )

    }

def test_factories_stack_a_source(identifiers, sources):
    factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
            }
        },
        sources=sources
    )

    factory.generate('TestCase')

    expected = markdown_to_df(
        '''
        | id   | first_name |
        | -    | -          |
        | {s1} | Buffy      |
        | {s2} | Willow     |
        '''.format(
            s1=identifiers['student'].record(case='TestCase', named_id='s1')['id'],
            s2=identifiers['student'].record(case='TestCase', named_id='s2')['id'],
        )
    )
    actual = sources['students'].data.drop(columns=sources['students'].defaults.keys())
    assert_frame_equal(actual, expected)

def test_factories_stack_sources(identifiers, sources):
    factory = Factory(
        data={
            'students': {
                'table': '''
                | id | organization_id | first_name |
                | -  | -               | -          |
                | s1 | o1              | Buffy      |
                | s2 | o1              | Willow     |
                ''',
            },
            'organizations': {
                'table': '''
                | id | name           |
                | -  | -              |
                | o1 | Sunnydale High |
                ''',
            }
        },
        sources=sources
    )

    factory.generate('TestCase')

    expected_students = markdown_to_df(
        '''
        | id   | organization_id | first_name |
        | -    | -               | -          |
        | {s1} | {o1}            | Buffy      |
        | {s2} | {o1}            | Willow     |
        '''.format(
            s1=identifiers['student'].record(case='TestCase', named_id='s1')['id'],
            s2=identifiers['student'].record(case='TestCase', named_id='s2')['id'],
            o1=identifiers['organization'].record(case='TestCase', named_id='o1')['id'],
        )
    )
    actual_students = sources['students'].data.drop(columns=['external_id'])

    expected_organizations = markdown_to_df(
        '''
        | id   | name           |
        | -    | -              |
        | {o1} | Sunnydale High |
        '''.format(
            o1=identifiers['organization'].record(case='TestCase', named_id='o1')['id'],
        )
    )
    actual_organizations = sources['organizations'].data.drop(columns=['uuid'])

    assert_frame_equal(actual_students, expected_students)
    assert_frame_equal(actual_organizations, expected_organizations)

def test_inheritance_wo_new_data(identifiers, sources):
    base_factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
            }
        },
        sources=sources
    )

    composite_factory = Factory(
        inherit_from=[base_factory],
        sources=sources
    )

    expected = base_factory.data['students']['dataframe']
    actual = composite_factory.data['students']['dataframe']
    assert_frame_equal(actual, expected)


def test_inheritance_w_new_data(identifiers, sources):
    base_factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
            }
        },
        sources=sources
    )

    modified_table = '''
        | id | first_name | last_name |
        | -  | -          | -         |
        | s1 | Buffy      | Summers   |
        | s2 | Xander     | Harris    |
    '''

    composite_factory = Factory(
        data={
            'students': { 'table': deepcopy(modified_table) }
        },
        inherit_from=[base_factory],
        sources=sources
    )

    expected = markdown_to_df(modified_table)
    actual = composite_factory.data['students']['dataframe']
    assert_frame_equal(actual, expected)

def test_inheritance_w_multiple_base_sources(identifiers, sources):
    base_factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
            },
            'organizations': {
                'table': '''
                | id | name           |
                | -  | -              |
                | o1 | Sunnydale High |
                ''',
            }
        },
        sources=sources
    )

    modified_table = '''
        | id | first_name | last_name |
        | -  | -          | -         |
        | s1 | Buffy      | Summers   |
        | s2 | Xander     | Harris    |
    '''

    composite_factory = Factory(
        data={
            'students': { 'table': deepcopy(modified_table) }
        },
        inherit_from=[base_factory],
        sources=sources
    )


    expected = markdown_to_df(modified_table)
    actual = composite_factory.data['students']['dataframe']
    assert_frame_equal(actual, expected)

    expected = base_factory.data['organizations']['dataframe']
    actual = composite_factory.data['organizations']['dataframe']
    assert_frame_equal(actual, expected)


def test_inheritance_w_multiple_composite_sources(identifiers, sources):
    base_factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
            },
        },
        sources=sources
    )

    modified_students_table = '''
        | id | first_name | last_name |
        | -  | -          | -         |
        | s1 | Buffy      | Summers   |
        | s2 | Xander     | Harris    |
    '''

    new_organizations_table = '''
        | id | name           |
        | -  | -              |
        | o1 | Sunnydale High |
    '''

    composite_factory = Factory(
        data={
            'students': { 'table': deepcopy(modified_students_table) },
            'organizations': { 'table': deepcopy(new_organizations_table) },
        },
        inherit_from=[base_factory],
        sources=sources
    )


    expected = markdown_to_df(modified_students_table)
    actual = composite_factory.data['students']['dataframe']
    assert_frame_equal(actual, expected)

    expected = markdown_to_df(new_organizations_table)
    actual = composite_factory.data['organizations']['dataframe']
    assert_frame_equal(actual, expected)


def test_multiple_inheritance(identifiers, sources):
    base1_factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
            },
        },
        sources=sources
    )

    base2_factory = Factory(
        data={
            'organizations': {
                'table': '''
                | id | name           |
                | -  | -              |
                | o1 | Sunnydale High |
                ''',
            }
        },
        sources=sources
    )

    composite_factory = Factory(
        inherit_from=[base1_factory, base2_factory],
        sources=sources
    )

    expected = base1_factory.data['students']['dataframe']
    actual = composite_factory.data['students']['dataframe']
    assert_frame_equal(actual, expected)

    expected = base2_factory.data['organizations']['dataframe']
    actual = composite_factory.data['organizations']['dataframe']
    assert_frame_equal(actual, expected)


def test_inheritance_defaults_are_overridden(identifiers, sources):
    base_factory = Factory(
        data={
            'students': {
                'table': '''
                | id |
                | -  |
                | s1 |
                ''',
                'values': {
                    'first_name': 'Bob'
                }
            }
        },
        sources=sources
    )

    composite_factory = Factory(
        data={
            'students': {
                'table': '''
                | id |
                | -  |
                | s1 |
                ''',
                'values': {
                    'last_name': 'Loblaw'
                }
            }
        },
        inherit_from=[base_factory],
        sources=sources
    )

    composite_factory.generate('TestCase')

    expected = markdown_to_df(
        '''
        | id   | first_name | last_name |
        | -    | -          | -         |
        | {s1} | Bob        | Loblaw    |
        '''.format(
            s1=identifiers['student'].record(case='TestCase', named_id='s1')['id'],
        )
    )
    actual = sources['students'].data.drop(columns=['external_id', 'organization_id'])
    assert_frame_equal(actual, expected)

def test_merge_data():
    data1 = {
        'students': {'table': 'a', 'dataframe': 'a', 'values': {'value_a1': 'a', 'X': 'a'}}
    }
    data2 = {
        'students': {'table': 'b', 'dataframe': 'b', 'values': {'value_b1': 'b', 'X': 'b'}},
        'organizations': {'table': 'b', 'dataframe': 'b'},

    }

    actual = Factory.merge_data(data1, data2)
    expected = {
        'students': {'table': 'b', 'dataframe': 'b', 'values': {'value_a1': 'a', 'X': 'b', 'value_b1': 'b'}},
        'organizations': {'table': 'b', 'dataframe': 'b'},
    }
    assert actual == expected

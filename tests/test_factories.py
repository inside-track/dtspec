import pytest

import dts
from dts.identifiers import Identifier
from dts.factories import Factory
from dts.sources import Source

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
            }
        )

    }

def test_factories_stack_sources(identifiers, sources):
    factory = Factory(
        data={
            'students': {
                'table': '''
                | id | first_name |
                | -  | -          |
                | s1 | Buffy      |
                | s2 | Willow     |
                ''',
                #TODO: values (defaults)
            }
        },
        sources=sources
    )

    factory.generate('TestCase')

    expected = dts.data.markdown_to_df(
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

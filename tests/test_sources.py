import pytest

import dts.identifiers
from dts.factories import Source

@pytest.fixture
def identifiers():
    return {
        'student': dts.identifiers.Identifier({
            'id': {'generator': 'unique_integer'}
        })
    }

def test_wtf(identifiers):
    s = Source(
        name='itk_api.students',
        data='''
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        ''',
        identifiers={
            'id': {
                'name': identifiers['student'],
                'attribute': 'id'
            }
        }
    )

import yaml
import jsonschema
import copy

import pytest

import dts.api

@pytest.fixture
def canonical_spec():
    return yaml.safe_load(open('tests/canonical_spec.yml'))

@pytest.fixture
def api(canonical_spec):
    return dts.api.Api(canonical_spec)


def test_canonical_spec_is_valid(canonical_spec):
    jsonschema.validate(canonical_spec, dts.api.SCHEMA)

def test_identifiers_are_defined(api):
    expected = {
        'students': dts.api.Identifier,
        'schools': dts.api.Identifier,
        'classes': dts.api.dts.api.Identifier
    }
    actual = {k:v.__class__ for k,v in api.spec['identifiers'].items()}
    assert actual == expected

def test_identifiers_have_attributes(api):
    expected = {'id', 'uuid', 'external_id'}
    actual = set(api.spec['identifiers']['students'].attributes.keys())
    assert actual == expected

def test_identifiers_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['identifiers'].append({
        'identifier': 'students',
        'attributes': [{'field': 'id', 'generator': 'unique_integer'}]
    })
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)



def test_sources_are_defined(api):
    expected = {
        'itk_api.students': dts.api.Source,
        'itk_api.schools': dts.api.Source,
        'itk_api.classes': dts.api.dts.api.Source
    }
    actual = {k:v.__class__ for k,v in api.spec['sources'].items()}
    assert actual == expected

def test_sources_can_have_defaults(api):
    expected = {'first_name': 'Bob'}
    actual = api.spec['sources']['itk_api.students'].defaults
    assert actual == expected

def test_sources_can_have_identifier_map(api):
    expected = api.spec['identifiers']['students']
    actual = api.spec['sources']['itk_api.students'].id_mapping['id']['identifier']
    assert actual == expected

def test_source_identifiers_must_exist(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['sources'].append({
        'source': 'not.students',
        'identifier_map': [
            {'column': 'id', 'identifier': {'name': 'does_not_compute', 'attribute': 'id'}}
        ]
    })
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)

def test_factories_are_defined(api):
    expected = {
        'CanonicalStudent': dts.api.Factory,
        'StudentWithClasses': dts.api.Factory,
    }
    actual = {k:v.__class__ for k,v in api.spec['factories'].items()}
    assert actual == expected

def test_factories_can_have_data_sources(api):
    expected = {'itk_api.students', 'itk_api.schools'}
    actual = api.spec['factories']['CanonicalStudent'].data.keys()
    assert actual == expected

def test_factories_inherit_from_parents(api):
    expected = {'itk_api.students', 'itk_api.schools', 'itk_api.classes'}
    actual = api.spec['factories']['StudentWithClasses'].data.keys()
    assert actual == expected

def test_factories_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['factories'].append({
        'factory': 'CanonicalStudent',
        'data': [
            {
                'source': 'itk_api.students',
                'table': '''
                    | student_id |
                    | -          |
                    | s1         |
                '''
            }
        ]
    })
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)

def test_factories_must_reference_known_sources(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['factories'].append({
        'factory': 'NoSource',
        'data': [
            {
                'source': 'not.students',
                'table': '''
                    | student_id |
                    | -          |
                    | s1         |
                '''
            }
        ]
    })
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)

def test_factory_parents_must_exist(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['factories'].append({
        'factory': 'AnotherOne',
        'parents': ['NotAFactory'],
        'data': [
            {
                'source': 'itk_api.students',
                'table': '''
                    | student_id |
                    | -          |
                    | s1         |
                '''
            }
        ]
    })
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)

def test_scenarios_are_defined(api):
    expected = {
        'DenormalizingStudentClasses': dts.api.Scenario
    }
    actual = {k:v.__class__ for k,v in api.spec['scenarios'].items()}
    assert actual == expected

def test_scenarios_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['scenarios'].append(copy.deepcopy(error_spec['scenarios'][0]))
    error_spec['scenarios'][-1]['description'] = 'Otherwise jsonschema complains of pure dupe'
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)

def test_scenarios_have_cases(api):
    expected = {
        'BasicDenormalization': dts.api.Case,
        'MissingClasses': dts.api.Case,
        'MultipleClasses': dts.api.Case,
    }
    actual = {k:v.__class__ for k,v in api.spec['scenarios']['DenormalizingStudentClasses'].cases.items()}
    assert actual == expected

def test_scenario_cases_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec['scenarios'][0]['cases'].append(copy.deepcopy(
        error_spec['scenarios'][0]['cases'][0]
    ))
    error_spec['scenarios'][0]['cases'][-1]['description'] = 'Otherwise jsonschema complains of pure dupe'
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)

def test_cases_inherit_from_scenario_factories(api):
    case = api.spec['scenarios']['DenormalizingStudentClasses'].cases['BasicDenormalization']

    expected = {'itk_api.students', 'itk_api.schools', 'itk_api.classes'}
    actual = case.factory.data.keys()
    assert actual == expected

# WIP - just have some whitespace to clean up
# def test_cases_can_customize_factories(api):
#     case = api.spec['scenarios']['DenormalizingStudentClasses'].cases['MissingClasses']

#     expected = '''
#         | student_id | name            |
#         | -          | -               |
#         | stu1       | Applied Stabby  |
#         | stu2       | Good Spells     |
#     '''
#     actual = case.factory.data['itk_api.classes']['table']
#     assert actual == expected

# TODO: def test_cases_have_expectations(api):


# Rename this file to test_api_specs.  Then create another one for testing api function

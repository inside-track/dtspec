import json
import copy

import yaml
import jsonschema
import pandas as pd

import pytest

import dts.api
from dts.data import markdown_to_df

from tests import assert_frame_equal

def transformer(students_df, schools_df, classes_df, dim_date_df, exclude_missing_classes=True):
    if exclude_missing_classes:
        classes_how = 'inner'
    else:
        classes_how = 'left'

    student_classes_df = students_df.rename(columns={'id': 'student_id'}).merge(
        schools_df.rename(columns={'id': 'school_id', 'name': 'school_name'}),
        how='inner',
        on='school_id'
    ).merge(
        classes_df.rename(columns={'name': 'class_name'}),
        how=classes_how,
        on='student_id'
    ).merge(
        dim_date_df.rename(columns={'date': 'start_date'}),
        how='left',
        on='start_date'
    )

    return {
        'analytics.student_classes': student_classes_df
    }

def run_transformer(dts_source_data, exclude_missing_classes=True):
    pd_data = {
        'students_df': pd.DataFrame().from_records(dts_source_data['raw.students']),
        'schools_df': pd.DataFrame().from_records(dts_source_data['raw.schools']),
        'classes_df': pd.DataFrame().from_records(dts_source_data['raw.classes']),
        'dim_date_df': pd.DataFrame().from_records(dts_source_data['analytics.dim_date']),
    }
    return transformer(**pd_data, exclude_missing_classes=exclude_missing_classes)


@pytest.fixture
def canonical_spec():
    return yaml.safe_load(open('tests/canonical_spec.yml'))

@pytest.fixture
def api(canonical_spec):
    return dts.api.Api(canonical_spec)

@pytest.fixture
def source_data(api):
    api.generate_sources()
    return api.source_data()

def test_source_data_stacks_for_every_case(api, source_data):
    actual = len(source_data['raw.schools'])
    n_cases = sum([len(scenario.cases) for scenario in api.spec['scenarios'].values()])
    expected = n_cases * 2
    assert actual == expected

def test_stacked_source_data_has_unique_identifiers(source_data):
    student_ids = [stu['id'] for stu in source_data['raw.students']]
    expected = len(student_ids)
    actual = len(set(student_ids))
    assert actual == expected

def test_source_without_identifer_not_stacked(source_data):
    actual = len(source_data['analytics.dim_date'])
    expected = 4
    assert actual == expected

# This might be overkill once the full test suite is up
def test_running_transformer_with_source_data(api, source_data):
    results = run_transformer(source_data)

    def stu(case_name, named_id):
        case_id = id(api.spec['scenarios']['DenormalizingStudentClasses'].cases[case_name])
        return api.spec['identifiers']['students'].record(
            case=case_id, named_id=named_id
        )['external_id']

    expected = markdown_to_df(
        f'''
        | external_id                          | first_name | last_name | school_name | class_name        | season       |
        | -                                    | -          | -         | -           | -                 | -            |
        | {stu("BasicDenormalization","stu1")} | Buffy      | Unknown   | Sunnydale   |  Applied Stabby   | Fall 2001    |
        | {stu("BasicDenormalization","stu2")} | Willow     | Unknown   | Sunnydale   |     Good Spells   | Spring 2002  |
        | {stu("BasicDenormalization","stu3")} | Bill       | Unknown   | San Dimas   |         Station   | Fall 2002    |
        | {stu("BasicDenormalization","stu4")} | Ted        | Unknown   | San Dimas   | Being Excellent   | Fall 2002    |
        | {stu("MissingClasses","stu1")}       | Buffy      | Unknown   | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | {stu("MissingClasses","stu2")}       | Willow     | Unknown   | Sunnydale   |     Good Spells   | Summer 2002  |
        | {stu("MultipleClasses","stu1")}      | Buffy      | Unknown   | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | {stu("MultipleClasses","stu2")}      | Willow     | Unknown   | Sunnydale   |     Good Spells   | Summer 2002  |
        | {stu("MultipleClasses","stu2")}      | Willow     | Unknown   | Sunnydale   | Season 6 Spells   | Summer 2002  |
        | {stu("MultipleClasses","stu3")}      | Bill       | Unknown   | San Dimas   |         Station   | Summer 2002  |
        | {stu("MultipleClasses","stu4")}      | Ted        | Unknown   | San Dimas   | Being Excellent   | Summer 2002  |
        | {stu("MultipleClasses","stu4")}      | Ted        | Unknown   | San Dimas   |         Station   | Summer 2002  |
        '''
    )

    actual = results['analytics.student_classes'][expected.columns]
    assert_frame_equal(actual, expected)


def test_actuals_are_loaded(api, source_data):
    results = run_transformer(source_data)
    api.load_actuals({
        target: json.loads(df.to_json(orient='records'))
        for target, df in results.items()
    })

    expected = markdown_to_df(
        '''
        | external_id | first_name | last_name | school_name | class_name        | season       |
        | -           | -          | -         | -           | -                 | -            |
        | stu1        | Buffy      | Unknown   | Sunnydale   |  Applied Stabby   | Fall 2001    |
        | stu2        | Willow     | Unknown   | Sunnydale   |     Good Spells   | Spring 2002  |
        | stu3        | Bill       | Unknown   | San Dimas   |         Station   | Fall 2002    |
        | stu4        | Ted        | Unknown   | San Dimas   | Being Excellent   | Fall 2002    |
        | stu1        | Buffy      | Unknown   | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | stu2        | Willow     | Unknown   | Sunnydale   |     Good Spells   | Summer 2002  |
        | stu1        | Buffy      | Unknown   | Sunnydale   |  Applied Stabby   | Summer 2002  |
        | stu2        | Willow     | Unknown   | Sunnydale   |     Good Spells   | Summer 2002  |
        | stu2        | Willow     | Unknown   | Sunnydale   | Season 6 Spells   | Summer 2002  |
        | stu3        | Bill       | Unknown   | San Dimas   |         Station   | Summer 2002  |
        | stu4        | Ted        | Unknown   | San Dimas   | Being Excellent   | Summer 2002  |
        | stu4        | Ted        | Unknown   | San Dimas   |         Station   | Summer 2002  |
        '''
    )

    actual = api.spec['targets']['analytics.student_classes'].data[expected.columns]
    assert_frame_equal(actual, expected)


def test_passing_expectation(api, source_data):
    results = run_transformer(source_data)
    api.load_actuals({
        target: json.loads(df.to_json(orient='records'))
        for target, df in results.items()
    })

    api.spec['scenarios']['DenormalizingStudentClasses'].cases['MissingClasses'].assert_expectations()

def test_failing_expectation(api, source_data):
    results = run_transformer(source_data, exclude_missing_classes=False)
    api.load_actuals({
        target: json.loads(df.to_json(orient='records'))
        for target, df in results.items()
    })

    with pytest.raises(AssertionError):
        api.spec['scenarios']['DenormalizingStudentClasses'].cases['MissingClasses'].assert_expectations()

def test_running_all_assertions(api, source_data):
    results = run_transformer(source_data)
    api.load_actuals({
        target: json.loads(df.to_json(orient='records'))
        for target, df in results.items()
    })

    api.run_assertions()

def test_running_all_assertions_with_failure(api, source_data):
    results = run_transformer(source_data, exclude_missing_classes=False)
    api.load_actuals({
        target: json.loads(df.to_json(orient='records'))
        for target, df in results.items()
    })

    with pytest.raises(AssertionError):
        api.run_assertions()

import copy

import yaml
import jsonschema

import pytest

import dts.api

# pylint: disable=redefined-outer-name


@pytest.fixture
def canonical_spec():
    all_specs = yaml.safe_load_all(open("tests/tutorial_spec.yml"))
    return {spec["description"].split("-")[0].strip(): spec for spec in all_specs}['Realistic']

@pytest.fixture
def api(canonical_spec):
    return dts.api.Api(canonical_spec)


def test_canonical_spec_is_valid(canonical_spec):
    jsonschema.validate(canonical_spec, dts.api.SCHEMA)


def test_identifiers_are_defined(api):
    expected = {
        "students": dts.api.Identifier,
        "schools": dts.api.Identifier,
    }
    actual = {k: v.__class__ for k, v in api.spec["identifiers"].items()}
    assert actual == expected


def test_identifiers_have_attributes(api):
    expected = {"id", "external_id"}
    actual = set(api.spec["identifiers"]["students"].attributes.keys())
    assert actual == expected


def test_identifiers_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["identifiers"].append(
        {
            "identifier": "students",
            "attributes": [{"field": "id", "generator": "unique_integer"}],
        }
    )
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)


def test_sources_are_defined(api):
    expected = {
        "raw_students": dts.api.Source,
        "raw_schools": dts.api.Source,
        "raw_classes": dts.api.Source,
        "dim_date": dts.api.Source,
    }
    actual = {k: v.__class__ for k, v in api.spec["sources"].items()}
    assert actual == expected


def test_sources_can_have_defaults(api):
    expected = {"start_date": "2002-06-01"}
    actual = api.spec["sources"]["raw_classes"].defaults
    assert actual == expected


def test_sources_can_have_identifier_map(api):
    expected = api.spec["identifiers"]["students"]
    actual = api.spec["sources"]["raw_students"].id_mapping["id"]["identifier"]
    assert actual == expected


def test_source_identifiers_must_exist(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["sources"].append(
        {
            "source": "not.students",
            "identifier_map": [
                {
                    "column": "id",
                    "identifier": {"name": "does_not_compute", "attribute": "id"},
                }
            ],
        }
    )
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)


def test_targets_are_defined(api):
    expected = {
        "student_classes": dts.api.Target,
        "students_per_school": dts.api.Target
    }
    actual = {k: v.__class__ for k, v in api.spec["targets"].items()}
    assert actual == expected


def test_targets_can_have_identifier_map(api):
    expected = api.spec["identifiers"]["students"]
    actual = api.spec["targets"]["student_classes"].id_mapping["card_id"][
        "identifier"
    ]
    assert actual == expected


def test_target_identifiers_must_exist(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["targets"].append(
        {
            "target": "not.students",
            "identifier_map": [
                {
                    "column": "external_id",
                    "identifier": {
                        "name": "does_not_compute",
                        "attribute": "external_id",
                    },
                }
            ],
        }
    )
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)


def test_factories_are_defined(api):
    expected = {
        "SomeStudents": dts.api.Factory,
        "StudentsWithClasses": dts.api.Factory,
        "DateDimension": dts.api.Factory,
    }
    actual = {k: v.__class__ for k, v in api.spec["factories"].items()}
    assert actual == expected


def test_factories_can_have_data_sources(api):
    expected = {"raw_students", "raw_schools"}
    actual = api.spec["factories"]["SomeStudents"].data.keys()
    assert actual == expected


def test_factories_inherit_from_parents(api):
    expected = {"raw_students", "raw_schools", "raw_classes"}
    actual = api.spec["factories"]["StudentsWithClasses"].data.keys()
    assert actual == expected


def test_factories_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["factories"].append(
        {
            "factory": "SomeStudents",
            "data": [
                {
                    "source": "raw_students",
                    "table": """
                    | student_id |
                    | -          |
                    | s1         |
                """,
                }
            ],
        }
    )
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)


def test_factories_must_reference_known_sources(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["factories"].append(
        {
            "factory": "NoSource",
            "data": [
                {
                    "source": "not.students",
                    "table": """
                    | student_id |
                    | -          |
                    | s1         |
                """,
                }
            ],
        }
    )
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)


def test_factory_parents_must_exist(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["factories"].append(
        {
            "factory": "AnotherOne",
            "parents": ["NotAFactory"],
            "data": [
                {
                    "source": "raw_students",
                    "table": """
                    | student_id |
                    | -          |
                    | s1         |
                """,
                }
            ],
        }
    )
    with pytest.raises(dts.api.ApiReferentialError):
        dts.api.Api(error_spec)


def test_scenarios_are_defined(api):
    expected = {
        "DenormalizingStudentClasses": dts.api.Scenario,
        "StudentAggregation": dts.api.Scenario
    }
    actual = {k: v.__class__ for k, v in api.spec["scenarios"].items()}
    assert actual == expected


def test_scenarios_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["scenarios"].append(copy.deepcopy(error_spec["scenarios"][0]))
    error_spec["scenarios"][-1][
        "description"
    ] = "Otherwise jsonschema complains of pure dupe"
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)


def test_scenarios_have_cases(api):
    expected = {
        "BasicDenormalization": dts.api.Case,
        "MissingClasses": dts.api.Case,
        "MultipleClasses": dts.api.Case,
    }
    actual = {
        k: v.__class__
        for k, v in api.spec["scenarios"]["DenormalizingStudentClasses"].cases.items()
    }
    assert actual == expected


def test_scenario_cases_cannot_be_duplicated(canonical_spec):
    error_spec = copy.deepcopy(canonical_spec)
    error_spec["scenarios"][0]["cases"].append(
        copy.deepcopy(error_spec["scenarios"][0]["cases"][0])
    )
    error_spec["scenarios"][0]["cases"][-1][
        "description"
    ] = "Otherwise jsonschema complains of pure dupe"
    with pytest.raises(dts.api.ApiDuplicateError):
        dts.api.Api(error_spec)


def test_cases_inherit_from_scenario_factories(api):
    case = api.spec["scenarios"]["DenormalizingStudentClasses"].cases[
        "BasicDenormalization"
    ]

    expected = {"raw_students", "raw_schools", "raw_classes", "dim_date"}
    actual = case.factory.data.keys()
    assert actual == expected


def test_cases_can_customize_factories(api):
    case = api.spec["scenarios"]["DenormalizingStudentClasses"].cases["MissingClasses"]

    expected = "\n".join(
        [
            v.strip()
            for v in """
        | student_id | name            |
        | -          | -               |
        | stu1       | Applied Stabby  |
        | stu2       | Good Spells     |
    """.split(
                "\n"
            )[
                1:
            ]
        ]
    )
    actual = case.factory.data["raw_classes"]["table"]
    assert actual == expected


def test_cases_have_data_expectations(api):
    case = api.spec["scenarios"]["DenormalizingStudentClasses"].cases[
        "BasicDenormalization"
    ]
    expected = [dts.api.DataExpectation]
    actual = [v.__class__ for v in case.expectations]
    assert actual == expected

import pytest

from dts.core import markdown_to_df, Identifier, Factory, Source, Target, Scenario, Case
from dts.expectations import DataExpectation

from tests import assert_frame_equal

# pylint: disable=redefined-outer-name


@pytest.fixture
def identifiers():
    return {
        "student": Identifier(
            {
                "id": {"generator": "unique_integer"},
                "external_id": {"generator": "unique_string"},
            }
        ),
        "organization": Identifier(
            {"id": {"generator": "unique_integer"}, "uuid": {"generator": "uuid"}}
        ),
    }


@pytest.fixture
def sources(identifiers):
    return {
        "students": Source(
            id_mapping={
                "id": {"identifier": identifiers["student"], "attribute": "id"},
                "organization_id": {
                    "identifier": identifiers["organization"],
                    "attribute": "id",
                },
            },
            defaults={
                "organization_id": {
                    "identifier": identifiers["organization"],
                    "attribute": "id",
                }
            },
        ),
        "organizations": Source(
            id_mapping={
                "id": {"identifier": identifiers["organization"], "attribute": "id"}
            }
        ),
    }


@pytest.fixture
def student_factory(sources):
    return Factory(
        data={
            "students": {
                "table": """
                | id | first_name |
                | -  | -          |
                | s1 | Bill       |
                | s2 | Ted        |
                """
            }
        },
        sources=sources,
    )


@pytest.fixture
def organization_factory(sources):
    return Factory(
        data={
            "organizations": {
                "table": """
                | id | name                    |
                | -  | -                       |
                | o1 | San Dimas High          |
                | o2 | Alaska Military Academy |
                """
            }
        },
        sources=sources,
    )


def test_scenarios_generate_case_data(identifiers, sources, student_factory):
    scenario = Scenario(
        cases={
            "SimpleStudent": Case(
                factory=Factory(sources=sources, inherit_from=[student_factory])
            )
        }
    )

    scenario.generate()

    expected = markdown_to_df(
        """
        | id   | first_name |
        | -    | -          |
        | {s1} | Bill       |
        | {s2} | Ted        |
        """.format(
            s1=identifiers["student"].generate(
                case=scenario.cases["SimpleStudent"], named_id="s1"
            )["id"],
            s2=identifiers["student"].generate(
                case=scenario.cases["SimpleStudent"], named_id="s2"
            )["id"],
        )
    )
    actual = sources["students"].data.drop(columns="organization_id")
    assert_frame_equal(actual, expected)


def test_scenarios_generate_case_data_over_multiple_cases(
    identifiers, sources, student_factory, organization_factory
):
    scenario = Scenario(
        cases={
            "SimpleStudent": Case(
                factory=Factory(sources=sources, inherit_from=[student_factory])
            ),
            "SimpleOrganization": Case(
                factory=Factory(sources=sources, inherit_from=[organization_factory])
            ),
        }
    )

    scenario.generate()

    expected = markdown_to_df(
        """
        | id   | first_name |
        | -    | -          |
        | {s1} | Bill       |
        | {s2} | Ted        |
        """.format(
            s1=identifiers["student"].generate(
                case=scenario.cases["SimpleStudent"], named_id="s1"
            )["id"],
            s2=identifiers["student"].generate(
                case=scenario.cases["SimpleStudent"], named_id="s2"
            )["id"],
        )
    )
    actual = sources["students"].data.drop(columns="organization_id")
    assert_frame_equal(actual, expected)

    expected = markdown_to_df(
        """
        | id   | name                    |
        | -    | -                       |
        | {o1} | San Dimas High          |
        | {o2} | Alaska Military Academy |
        """.format(
            o1=identifiers["organization"].generate(
                case=scenario.cases["SimpleOrganization"], named_id="o1"
            )["id"],
            o2=identifiers["organization"].generate(
                case=scenario.cases["SimpleOrganization"], named_id="o2"
            )["id"],
        )
    )
    actual = sources["organizations"].data
    assert_frame_equal(actual, expected)


def test_scenario_case_factories_can_override(
    identifiers, sources, student_factory, organization_factory
):
    scenario = Scenario(
        cases={
            "StudentOrg": Case(
                factory=Factory(
                    sources=sources,
                    inherit_from=[student_factory, organization_factory],
                    data={
                        "students": {
                            "table": """
                            | id | organization_id | first_name |
                            | -  | -               | -          |
                            | s1 | o1              | Bill       |
                            | s2 | o1              | Ted        |
                            """
                        }
                    },
                )
            )
        }
    )

    scenario.generate()

    expected = markdown_to_df(
        """
        | id   | organization_id | first_name |
        | -    | -               | -          |
        | {s1} | {o1}            | Bill       |
        | {s2} | {o1}            | Ted        |
        """.format(
            s1=identifiers["student"].generate(
                case=scenario.cases["StudentOrg"], named_id="s1"
            )["id"],
            s2=identifiers["student"].generate(
                case=scenario.cases["StudentOrg"], named_id="s2"
            )["id"],
            o1=identifiers["organization"].generate(
                case=scenario.cases["StudentOrg"], named_id="o1"
            )["id"],
        )
    )
    actual = sources["students"].data
    assert_frame_equal(actual, expected)

    expected = markdown_to_df(
        """
        | id   | name                    |
        | -    | -                       |
        | {o1} | San Dimas High          |
        | {o2} | Alaska Military Academy |
        """.format(
            o1=identifiers["organization"].generate(
                case=scenario.cases["StudentOrg"], named_id="o1"
            )["id"],
            o2=identifiers["organization"].generate(
                case=scenario.cases["StudentOrg"], named_id="o2"
            )["id"],
        )
    )
    actual = sources["organizations"].data
    assert_frame_equal(actual, expected)


def test_scenarios_stack_case_data(identifiers, sources, student_factory):
    scenario = Scenario(
        cases={
            "SimpleStudent": Case(
                factory=Factory(sources=sources, inherit_from=[student_factory])
            ),
            "AltStudent": Case(
                factory=Factory(
                    sources=sources,
                    inherit_from=[student_factory],
                    data={
                        "students": {
                            "table": """
                            | id | first_name |
                            | -  | -          |
                            | s1 | Napoleon   |
                            """
                        }
                    },
                )
            ),
        }
    )

    scenario.generate()

    expected = markdown_to_df(
        """
        | id    | first_name |
        | -     | -          |
        | {s1}  | Bill       |
        | {s2}  | Ted        |
        | {as1} | Napoleon   |
        """.format(
            s1=identifiers["student"].generate(
                case=scenario.cases["SimpleStudent"], named_id="s1"
            )["id"],
            s2=identifiers["student"].generate(
                case=scenario.cases["SimpleStudent"], named_id="s2"
            )["id"],
            as1=identifiers["student"].generate(
                case=scenario.cases["AltStudent"], named_id="s1"
            )["id"],
        )
    )
    actual = sources["students"].data.drop(columns="organization_id")
    assert_frame_equal(actual, expected)


def test_cases_assert_expectations():  # (sources, student_factory):
    table = """
        | id | name   |
        | -  | -      |
        | 1  | Buffy  |
        | 2  | Willow |
        | 3  | Xander |
    """

    actual_data = markdown_to_df(table)
    actual_data["name"].iloc[1] = "Evil Willow"

    expectation = DataExpectation(Target(), table)
    expectation.load_actual(actual_data)

    case = Case(expectations=[expectation])
    with pytest.raises(AssertionError):
        case.assert_expectations()

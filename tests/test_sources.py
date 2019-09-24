import pytest

import dtspec.core
from dtspec.core import markdown_to_df, Identifier, Source, Case

from tests import assert_frame_equal

# pylint: disable=redefined-outer-name


@pytest.fixture
def identifiers():
    return {
        "student": Identifier(
            {"id": {"generator": "unique_integer"}, "uuid": {"generator": "uuid"}}
        ),
        "organization": Identifier(
            {"id": {"generator": "unique_integer"}, "uuid": {"generator": "uuid"}}
        ),
    }


@pytest.fixture
def simple_source(identifiers):
    return Source(
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}}
    )


@pytest.fixture
def cases():
    return [Case(name="TestCase1"), Case(name="TestCase2")]


def test_identifers_are_translated(simple_source, identifiers, cases):
    simple_source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
    )

    actual = simple_source.data
    expected = markdown_to_df(
        """
        | id   | first_name |
        | -    | -          |
        | {s1} | Bob        |
        | {s2} | Nancy      |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
        )
    )
    assert_frame_equal(actual, expected)


def test_sources_stack(simple_source, identifiers, cases):
    simple_source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
    )

    simple_source.stack(
        cases[1],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bobob      |
        | s2 | Nanci      |
        """
        ),
    )

    actual = simple_source.data
    expected = markdown_to_df(
        """
        | id    | first_name |
        | -     | -          |
        | {s11} | Bob        |
        | {s12} | Nancy      |
        | {s21} | Bobob      |
        | {s22} | Nanci      |
        """.format(
            s11=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s12=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
            s21=identifiers["student"].generate(case=cases[1], named_id="s1")["id"],
            s22=identifiers["student"].generate(case=cases[1], named_id="s2")["id"],
        )
    )
    assert_frame_equal(actual, expected)


def test_data_converts_to_json(simple_source, identifiers, cases):
    simple_source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
    )

    actual = simple_source.serialize()
    expected = [
        {
            "id": identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            "first_name": "Bob",
        },
        {
            "id": identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
            "first_name": "Nancy",
        },
    ]

    assert actual == expected


def test_setting_defaults(identifiers, cases):
    source = Source(
        defaults={"last_name": "Jones"},
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}},
    )

    source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
    )

    actual = source.data
    expected = markdown_to_df(
        """
        | id   | first_name | last_name |
        | -    | -          | -         |
        | {s1} | Bob        | Jones     |
        | {s2} | Nancy      | Jones     |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
        )
    )
    assert_frame_equal(actual, expected)


def test_overriding_defaults(identifiers, cases):
    source = Source(
        defaults={"last_name": "Jones"},
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}},
    )

    source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name | last_name |
        | -  | -          | -         |
        | s1 | Bob        | Not Jones |
        | s2 | Nancy      | Not Jones |
        """
        ),
    )

    actual = source.data
    expected = markdown_to_df(
        """
        | id   | first_name | last_name |
        | -    | -          | -         |
        | {s1} | Bob        | Not Jones |
        | {s2} | Nancy      | Not Jones |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
        )
    )
    assert_frame_equal(actual, expected)


def test_identifier_defaults(identifiers, cases):
    source = Source(
        defaults={
            "organization_id": {
                "identifier": identifiers["organization"],
                "attribute": "id",
            }
        },
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}},
    )

    source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
    )

    anonymous_ids = [
        v["id"]
        for v in identifiers["organization"].cached_ids[id(cases[0])].named_ids.values()
    ]

    actual = source.data
    expected = markdown_to_df(
        """
        | id   | first_name | organization_id |
        | -    | -          | -               |
        | {s1} | Bob        | {o1}            |
        | {s2} | Nancy      | {o2}            |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
            o1=anonymous_ids[0],
            o2=anonymous_ids[1],
        )
    )
    assert_frame_equal(actual, expected)


def test_setting_values(identifiers, cases):
    source = Source(
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}}
    )

    source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
        values={"last_name": "Summers"},
    )

    actual = source.data
    expected = markdown_to_df(
        """
        | id   | first_name | last_name |
        | -    | -          | -         |
        | {s1} | Bob        | Summers   |
        | {s2} | Nancy      | Summers   |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
        )
    )
    assert_frame_equal(actual, expected)


def test_setting_defaults_and_values(identifiers, cases):
    source = Source(
        defaults={"last_name": "Jones", "gender": "X"},
        id_mapping={"id": {"identifier": identifiers["student"], "attribute": "id"}},
    )

    source.stack(
        cases[0],
        markdown_to_df(
            """
        | id | first_name |
        | -  | -          |
        | s1 | Bob        |
        | s2 | Nancy      |
        """
        ),
        values={"last_name": "Summers"},
    )

    actual = source.data
    expected = markdown_to_df(
        """
        | id   | first_name | last_name | gender |
        | -    | -          | -         | -      |
        | {s1} | Bob        | Summers   | X      |
        | {s2} | Nancy      | Summers   | X      |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
        )
    )
    assert_frame_equal(actual, expected)


@pytest.fixture
def source_w_multiple_ids(identifiers):
    return Source(
        id_mapping={
            "id": {"identifier": identifiers["student"], "attribute": "id"},
            "uuid": {"identifier": identifiers["student"], "attribute": "uuid"},
            "organization_id": {
                "identifier": identifiers["organization"],
                "attribute": "id",
            },
        }
    )


def test_multiple_identifers_are_translated(source_w_multiple_ids, identifiers, cases):
    source_w_multiple_ids.stack(
        cases[0],
        markdown_to_df(
            """
        | id | uuid | organization_id |first_name  |
        | -  | -    | -               | -          |
        | s1 | s1   | o1              | Bob        |
        | s2 | s2   | o1              | Nancy      |
        """
        ),
    )

    actual = source_w_multiple_ids.data
    expected = markdown_to_df(
        """
        | id   | uuid  | organization_id | first_name |
        | -    | -     | -               | -          |
        | {s1} | {su1} | {o1}            | Bob        |
        | {s2} | {su2} | {o1}            | Nancy      |
        """.format(
            s1=identifiers["student"].generate(case=cases[0], named_id="s1")["id"],
            s2=identifiers["student"].generate(case=cases[0], named_id="s2")["id"],
            su1=identifiers["student"].generate(case=cases[0], named_id="s1")["uuid"],
            su2=identifiers["student"].generate(case=cases[0], named_id="s2")["uuid"],
            o1=identifiers["organization"].generate(case=cases[0], named_id="o1")["id"],
        )
    )
    assert_frame_equal(actual, expected)


def test_all_identifying_columns_must_be_present(source_w_multiple_ids, cases):
    with pytest.raises(dtspec.core.IdentifierWithoutColumnError) as excinfo:
        source_w_multiple_ids.stack(
            cases[0],
            markdown_to_df(
                """
            | id | first_name  |
            | -  | -           |
            | s1 | Bob         |
            | s2 | Nancy       |
            """
            ),
        )

    # Error message contains a readable case name
    assert "TestCase1" in str(excinfo.value).split("\n")[0]


def test_source_without_identifier_generates_data(cases):
    table = """
        | date       | season      |
        | -          | -           |
        | 2001-09-08 | Fall 2001   |
        | 2002-01-09 | Spring 2002 |
    """

    source = Source()
    source.stack(cases[0], markdown_to_df(table))

    actual = source.data
    expected = markdown_to_df(table)
    assert_frame_equal(actual, expected)


def test_source_without_identifer_not_stacked(cases):
    table = """
        | date       | season      |
        | -          | -           |
        | 2001-09-08 | Fall 2001   |
        | 2002-01-09 | Spring 2002 |
    """

    source = Source()
    source.stack(cases[0], markdown_to_df(table))
    source.stack(cases[0], markdown_to_df(table))

    actual = source.data
    expected = markdown_to_df(table)
    assert_frame_equal(actual, expected)


def test_source_without_identifer_raises_if_data_changes(cases):
    source = Source()
    source.stack(
        cases[0],
        markdown_to_df(
            """
            | date       | season      |
            | -          | -           |
            | 2001-09-08 | Fall 2001   |
            | 2002-01-09 | Spring 2002 |
        """
        ),
    )

    with pytest.raises(dtspec.core.CannotStackStaticSourceError) as excinfo:
        source.stack(
            cases[0],
            markdown_to_df(
                """
                | date       | season      |
                | -          | -           |
                | 2002-06-01 | Summer 2002 |
                | 2002-09-07 | Fall 2002   |
            """
            ),
        )

    # Error message contains a readable case name
    assert "TestCase1" in str(excinfo.value).split("\n")[0]

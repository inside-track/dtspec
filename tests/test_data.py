import pandas as pd
from pandas.util.testing import assert_frame_equal

from dts.core import markdown_to_df


class TestMarkdownToDf:
    def test_convert_table_to_df(self):
        given = """
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | two   |
            | 3  | three |
            """

        expected = pd.DataFrame(
            {"id": ["1", "2", "3"], "name": ["one", "two", "three"]}
        )
        actual = markdown_to_df(given)

        assert_frame_equal(actual, expected)

    def test_convert_table_to_df_with_blanks(self):
        given = """
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  |       |
            | 3  | three |
            """

        expected = pd.DataFrame({"id": ["1", "2", "3"], "name": ["one", "", "three"]})
        actual = markdown_to_df(given)

        assert_frame_equal(actual, expected)

    def test_convert_table_to_df_with_nulls(self):
        given = """
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | #NULL |
            | 3  | three |
            """

        expected = pd.DataFrame({"id": ["1", "2", "3"], "name": ["one", None, "three"]})
        actual = markdown_to_df(given)

        assert_frame_equal(actual, expected)

    def test_ignores_trailing_comments(self):
        given = """
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | two   | # Some comment
            | 3  | three |
            """

        expected = pd.DataFrame(
            {"id": ["1", "2", "3"], "name": ["one", "two", "three"]}
        )
        actual = markdown_to_df(given)

        assert_frame_equal(actual, expected)

    def test_honors_embedded_octothorpes(self):

        given = """
            | id | name  |
            | -  | -     |
            | 1  | one   |
            | 2  | #2    |
            | 3  | three |
            """

        expected = pd.DataFrame({"id": ["1", "2", "3"], "name": ["one", "#2", "three"]})
        actual = markdown_to_df(given)

        assert_frame_equal(actual, expected)

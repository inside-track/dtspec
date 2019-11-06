import pandas.util.testing

from dtspec.core import (
    markdown_to_df,
    BadMarkdownTableError,
    translate_embedded_identifiers,
)


def assert_frame_equal(actual, expected, **kwargs):
    try:
        pandas.util.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as err:
        msg = str(err)
        msg += "\nActual:\n{}".format(actual)
        msg += "\nExpected:\n{}".format(expected)
        raise AssertionError(msg)


class MissingExpectedKeysAssertionError(Exception):
    pass


class DataExpectation:
    def __init__(
        self, target, table, values=None, by=None, compare_via=None, identifiers=None
    ):
        """
        Compares actual results with a table of expected data.

        Args:
            target (str): Name of data target
            table (str): Markdown representation of expected data
            values (dict): Dictionary where the keys are column names and the values are
                           the expected values of that column (constant across all records in case).
            by (array): List of fields to do sorted or key-based comparison.
            compare_via (str): Choose either "exact", "sorted", or "keys".
                * If "exact", then target data must exactly match expected format
                  for all of the fields listed (e.g., sort order matters).
                  This is the default if there is no *by* option specified.
                * If "sorted", then expected and actuals are sorted by the *by* keys
                  prior to comparison.  This is the default if *by* keys are specified.
                * If "keys", then any actual data that does not have values specified
                  by the *by* keys are ignored (think: compared by expected left join actual).
        """

        self.target = target
        self.values = values or {}
        self.actual_data = None
        self.by = by or []
        self.identifiers = identifiers or {}

        if len(self.by) > 0:
            self.compare_via = compare_via or "sorted"
        else:
            self.compare_via = compare_via or "exact"

        if len(self.by) == 0 and self.compare_via != "exact":
            raise ValueError(
                f'Cannot use compare_via={self.compare_via} without a "by" option'
            )
        self.expected_data = self._build_expected_data(table)

    def _build_expected_data(self, table):
        try:
            expected_df = markdown_to_df(table)
        except BadMarkdownTableError as err:
            raise BadMarkdownTableError(
                f"Unable to generate data for target {self.target}:\n{err}"
            )

        self._add_constants(expected_df)
        return expected_df

    def _add_constants(self, df):
        for column, value in self.values.items():
            df[column] = value

    def load_actual(self, actual_data):
        self.actual_data = actual_data

    def assert_expected(self, case):
        self.expected_data = translate_embedded_identifiers(
            self.expected_data, case, self.identifiers
        )

        if self.compare_via == "exact":
            expected = self.expected_data.reset_index(drop=True)
            actual = self.actual_data.reset_index(drop=True)
        elif self.compare_via == "sorted":
            expected = self.expected_data.sort_values(self.by).reset_index(drop=True)
            actual = self.actual_data.sort_values(self.by).reset_index(drop=True)
        elif self.compare_via == "keys":
            expected = self.expected_data.sort_values(self.by).reset_index(drop=True)

            merged = self.actual_data.merge(
                expected[self.by],
                how="outer",
                on=self.by,
                indicator="__dtspec_indicator__",
            ).sort_values(self.by)

            actual = (
                merged.query('__dtspec_indicator__ == "both"')
                .drop(columns="__dtspec_indicator__")
                .reset_index(drop=True)
            )

            missing_expected_keys = merged.query(
                '__dtspec_indicator__ == "right_only"'
            ).copy()[self.by]
            missing_actual_keys = merged.query(
                '__dtspec_indicator__ == "left_only"'
            ).copy()[self.by]
            if len(missing_expected_keys) > 0:
                raise MissingExpectedKeysAssertionError(
                    f"For target {self.target.name}, keys in expected data, not found in actual data:\n{missing_expected_keys}\n"
                    + f"Keys in actual data, not in expected:\n{missing_actual_keys}"
                )
        else:
            raise ValueError(f"Unknown compare_via option: {self.compare_via}")

        comparison_columns = expected.columns
        missing_expected_columns = set(comparison_columns) - set(actual.columns)
        if len(missing_expected_columns) > 0:
            raise AssertionError(
                f"Target {self.target.name} missing expected columns: {missing_expected_columns}"
            )

        assert_frame_equal(
            actual[comparison_columns], expected, check_names=False, check_dtype=False
        )

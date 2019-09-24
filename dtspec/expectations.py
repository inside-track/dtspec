import pandas.util.testing

from dtspec.core import markdown_to_df


def assert_frame_equal(actual, expected, **kwargs):
    try:
        pandas.util.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as err:
        msg = str(err)
        msg += "\nActual:\n{}".format(actual)
        msg += "\nExpected:\n{}".format(expected)
        raise AssertionError(msg)


class DataExpectation:
    def __init__(self, target, table, by=None):
        self.target = target
        self.expected_data = markdown_to_df(table)
        self.actual_data = None
        self.by = by or []

    def load_actual(self, actual_data):
        self.actual_data = actual_data

    def assert_expected(self):
        if self.by:
            expected = self.expected_data.sort_values(self.by).reset_index(drop=True)
            actual = self.actual_data.sort_values(self.by).reset_index(drop=True)
        else:
            expected = self.expected_data.reset_index(drop=True)
            actual = self.actual_data.reset_index(drop=True)

        comparison_columns = expected.columns
        missing_expected_columns = set(comparison_columns) - set(actual.columns)
        if len(missing_expected_columns) > 0:
            raise AssertionError(
                f"Missing expected columns: {missing_expected_columns}"
            )

        assert_frame_equal(
            actual[comparison_columns], expected, check_names=False, check_dtype=False
        )

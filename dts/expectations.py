import pandas.util.testing

import dts.data

def assert_frame_equal(actual, expected, **kwargs):
    try:
        pandas.util.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as err:
        msg = str(err)
        msg += '\nActual:\n{}'.format(actual)
        msg += '\nExpected:\n{}'.format(expected)
        raise AssertionError(msg)

class DataExpectation:
    def __init__(self, target, table, by=None):
        self.target = target
        self.expected_data = dts.data.markdown_to_df(table)
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

        assert_frame_equal(actual, expected, check_names=False, check_dtype=False)

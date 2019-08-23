import pandas.util.testing

def assert_frame_equal(actual, expected, **kwargs):
    try:
        pandas.util.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as err:
        msg = str(err)
        msg += '\nActual:\n{}'.format(actual)
        msg += '\nExpected:\n{}'.format(expected)
        raise AssertionError(msg)

import time
from functools import wraps


def retry(exceptions, tries=4, delay=3, backoff=2):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
    """

    def deco_retry(fun):
        @wraps(fun)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return fun(*args, **kwargs)
                except exceptions as err:
                    msg = f"{err}, Retrying in {mdelay} seconds..."
                    print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return fun(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

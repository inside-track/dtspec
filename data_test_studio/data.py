import re
import io

import pandas as pd

class InvalidHeaderSeparatorError(Exception): pass

def markdown_to_df(markdown):
    cleaned = markdown

    # Remove trailing comments
    cleaned = re.compile(r'(#[^\|]*$)', flags=re.MULTILINE).sub('', cleaned)

    # Remove whitespace surrouding pipes
    cleaned = re.compile(r'[ \t]*\|[ \t]*').sub('|', cleaned)

    # Remove beginning and terminal pipe on each row
    cleaned = re.compile(r'(^\s*\|\s*|\s*\|\s*$)', flags=re.MULTILINE).sub('', cleaned)

    # Split by newlines
    cleaned = cleaned.split('\n')

    # Remove header separator
    header_separator = cleaned.pop(1)
    if re.search(re.compile(r'^[\s\-\|]*$'), header_separator) is None:
        raise InvalidHeaderSeparatorError('Bad header separator: {}'.format(header_separator))

    # Unsplit
    cleaned = '\n'.join(cleaned)

    df = pd.read_csv(
        io.StringIO(cleaned),
        sep='|',
        na_values='#NULL',
        keep_default_na=False,
        dtype=str
    )

    return df

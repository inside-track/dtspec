import io
import re
import random
import uuid
import json
import copy

from types import SimpleNamespace

import pandas as pd
from pandas.util.testing import assert_frame_equal

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


class UniqueIdGenerator: #pylint: disable=too-few-public-methods
    '''
    Class used to build id generators.
    fmt - A function that accepts a single integer argument and returns a value to be used as an id.

    Example:
      students = UniqueIdGenerator(lambda i: 'S{}'.format(i))
      [next(students) for x in range(10)]
      #=> ['S6', 'S5', 'S3', 'S1', 'S4', 'S7', 'S9', 'S2', 'S8', 'S16']

    This generator also supports the call method, which operates the same as ``next``.

    Example:
      [UniqueIdGenerator()() for x in range(10)]
      #=> ['S6', 'S5', 'S3', 'S1', 'S4', 'S7', 'S9', 'S2', 'S8', 'S16']
    '''
    def __init__(self, fmt=int):
        self.fmt = fmt
        self.size = 1
        self.gen_sample()

    def gen_sample(self):
        self.sample = list(range(10**(self.size-1), 10**self.size))
        random.shuffle(self.sample)
        self.size += 1

    def __next__(self):
        i = self.sample.pop()
        if len(self.sample) == 0:
            self.gen_sample()
        return self.fmt(i)

    def __call__(self):
        return next(self)


class IdGenerators:
    'IdGenerators contain methods that return functions that when called generate identifier values'

    @staticmethod
    def unique_integer():
        return UniqueIdGenerator(str)

    @staticmethod
    def unique_string(prefix=''):
        return UniqueIdGenerator(lambda i: '{}{}'.format(prefix, i))

    @staticmethod
    def uuid():
        return lambda: str(uuid.uuid4())

class UnableToFindNamedIdError(Exception): pass
class Identifier:
    def __init__(self, attributes):
        self.attributes = attributes
        self.cases = {}

        self.generators = {}
        for attr, props in self.attributes.items():
            generator_args = {k:v for k,v in props.items() if k != 'generator'}
            self.generators[attr] = getattr(IdGenerators, props['generator'])(**generator_args)

    # TODO: rename to generate
    def record(self, case, named_id):
        if case not in self.cases:
            self.cases[case] = SimpleNamespace(named_ids={})

        if named_id not in self.cases[case].named_ids:
            self.cases[case].named_ids[named_id] = {}
            for attr, generator in self.generators.items():
                self.cases[case].named_ids[named_id][attr] = generator()

        return self.cases[case].named_ids[named_id]

    # TODO: raise an error if a record belongs to more than one case?
    def find(self, attribute, raw_id):
        'Given an attribute and a raw id, return named attribute and case'
        found = SimpleNamespace(named_id=None, case=None)
        for case_name, case in self.cases.items():
            for named_id, attributes in case.named_ids.items():
                if attributes[attribute] == raw_id:
                    found.named_id = named_id
                    found.case = case_name
                    return found

        raise UnableToFindNamedIdError(
            f'Unable to find named identifier for attribute "{attribute}" and value "{raw_id}"'
        )



def _frame_is_equal(df1, df2):
    try:
        assert_frame_equal(df1, df2)
    except AssertionError as err:
        return False
    return True

class IdentifierWithoutColumnError(Exception): pass
class CannotStackStaticSourceError(Exception): pass

class Source:
    def __init__(self, defaults=None, id_mapping=None):
        self.defaults = defaults
        self.id_mapping = id_mapping
        self.data = pd.DataFrame()


    def stack(self, case, data, values=None):
        'values override defaults at stack time'

        w_defaults_df = self._add_defaults(data, case, values)
        if self.id_mapping:
            translated_df = self._translate_identifiers(w_defaults_df, case)
            self.data = pd.concat([self.data, translated_df], sort=False).reset_index(drop=True)
        else:
            if len(self.data) > 0 and not _frame_is_equal(self.data, w_defaults_df):
                raise CannotStackStaticSourceError(
                    f'In case f"{case}", attempting to stack data onto source without identifiers:\n {data}'
                )
            self.data = w_defaults_df

    def _add_defaults(self, df, case, values):
        default_values = {**(self.defaults or {}), **(values or {})}
        if len(default_values) == 0:
            return df

        for column, value in default_values.items():
            if column in df.columns:
                continue

            if isinstance(value, dict) and 'identifier' in value:
                df[column] = None
                df[column] = df[column].apply(
                    lambda _:
                    value['identifier'].record(case=case, named_id=uuid.uuid4())[value['attribute']]
                )
            else:
                df[column] = value

        return df

    def _translate_identifiers(self, df, case):
        missing_columns = set(self.id_mapping.keys()) - set(df.columns)
        if len(missing_columns) > 0:
            raise IdentifierWithoutColumnError(
                f'In case "{case}", data source is missing columns corresponding to identifier attributes: {missing_columns}'
            )


        for column, mapto in self.id_mapping.items():
            df[column] = df[column].apply(
                lambda v: mapto['identifier'].record(case=case, named_id=v)[mapto['attribute']]
            )
        return df

    def serialize(self, orient='records'):
        return json.loads(self.data.to_json(orient=orient))


class Target:
    def __init__(self, id_mapping=None):
        self.id_mapping = id_mapping or {}
        self.data = pd.DataFrame()

    def load_actual(self, records):
        self.data = pd.DataFrame.from_records(records)
        self._translate_identifiers()

    def _translate_identifiers(self):
        for column, mapto in self.id_mapping.items():
            if column not in self.data:
                raise KeyError(
                    f'Target defines identifier map for column "{column}", '
                    f'but "{column}" not found in actual data.  '
                    f'columns found: {self.data.columns}'
                )

            lkp = {
                raw_id: mapto['identifier'].find(mapto['attribute'], raw_id)
                for raw_id in self.data[column]
            }
            self.data['__dts_case__'] = self.data[column].apply(lambda v: lkp[v].case)
            self.data[column] = self.data[column].apply(lambda v: lkp[v].named_id)

    def case_data(self, case):
        if '__dts_case__' not in self.data.columns:
            return self.data

        return (
            self.data[self.data['__dts_case__'] == case]
            .drop(columns='__dts_case__')
            .reset_index(drop=True)
        )



class Factory:
    def __init__(self, data=None, sources=None, inherit_from=None):
        self.data = data or {}
        self._parse_tables()
        self._compose_data(inherit_from)
        self.sources = sources

    def generate(self, case):
        for source_name in self.data.keys():
            source = self.sources[source_name]
            source.stack(
                case=case,
                data=self.data[source_name]['dataframe'],
                values={
                    **(source.defaults or {}),
                    **self.data[source_name].get('values', {})
                }
            )

    @staticmethod
    def merge_data(data1, data2):
        data1 = copy.deepcopy(data1)
        data2 = copy.deepcopy(data2)
        merged = {**data1, **data2}
        for source_name in merged.keys():
            if 'values' in merged[source_name]:
                merged[source_name]['values'] = {
                    **data1.get(source_name, {}).get('values', {}),
                    **data2.get(source_name, {}).get('values', {})
                }
        return merged

    def _parse_tables(self):
        for source_name, source_def in self.data.items():
            self.data[source_name]['dataframe'] = markdown_to_df(source_def['table'])

    def _compose_data(self, inherit_from):
        if inherit_from is None:
            return

        factories = inherit_from + [self]
        composed_data = factories.pop(0).data
        for factory in factories:
            composed_data = self.merge_data(composed_data, factory.data)
        self.data = composed_data


class DuplicateCaseError(Exception): pass
class Scenario:
    def __init__(self, cases=None):
        self.cases = cases or {}

    def generate(self):
        for case_name, case in self.cases.items():
            case.factory.generate(id(case))


class Case:
    def __init__(self, factory=None, expectations=None):
        self.factory = factory
        self.expectations = expectations or []

    def assert_expectations(self):
        for expectation in self.expectations:
            expectation.load_actual(expectation.target.case_data(id(self)))
            expectation.assert_expected()

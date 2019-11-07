import io
import re
import random
import uuid
import json
import copy

from types import SimpleNamespace

import pandas as pd
from pandas.util.testing import assert_frame_equal

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 200)

NULL_TOKEN = "{NULL}"


class InvalidHeaderSeparatorError(Exception):
    pass


class BadMarkdownTableError(Exception):
    pass


def _clean_markdown(markdown):
    cleaned = copy.copy(markdown)

    # Remove trailing comments
    cleaned = re.compile(r"(#[^\|]*$)", flags=re.MULTILINE).sub("", cleaned)

    # Remove whitespace surrouding pipes
    cleaned = re.compile(r"[ \t]*\|[ \t]*").sub("|", cleaned)

    # Remove beginning and terminal pipe on each row
    cleaned = re.compile(r"(^\s*\|\s*|\s*\|\s*$)", flags=re.MULTILINE).sub("", cleaned)

    # Split by newlines
    cleaned = cleaned.split("\n")

    # Remove header separator
    header_separator = cleaned.pop(1)
    if re.search(re.compile(r"^[\s\-\|]*$"), header_separator) is None:
        raise InvalidHeaderSeparatorError(
            "Bad header separator: {}".format(header_separator)
        )

    # Unsplit
    cleaned = "\n".join(cleaned)

    return cleaned


def markdown_to_df(markdown):
    try:
        cleaned = _clean_markdown(markdown)
    except (TypeError, InvalidHeaderSeparatorError) as err:
        raise BadMarkdownTableError(
            f"Unabled to parse markdown table:\n{markdown}\n\n" + f"Reason: {err}"
        )

    try:
        df = pd.read_csv(
            io.StringIO(cleaned), sep="|", keep_default_na=False, dtype=str
        )
    except pd.errors.ParserError as err:
        raise BadMarkdownTableError(
            f"Unable to parse markdown table:\n{markdown}\n\n" + f"Reason: {err}"
        )

    return df


def translate_embedded_identifiers(df, case, identifiers, identifier_regex=None):
    identifier_regex = identifier_regex or re.compile(
        r"\{(?P<identifier>\w+)\.(?P<attribute>\w+)\[(?P<named_id>[^\[\]]+)\]\}"
    )

    def translate_id(v):
        if not isinstance(v, str):
            return v

        re_match = identifier_regex.finditer(v)
        if not re_match:
            return v

        for imatch in re_match:
            try:
                translated_id = identifiers[imatch["identifier"]].generate(
                    case=case, named_id=imatch["named_id"]
                )[imatch["attribute"]]
            except KeyError as _err:
                raise UnableToFindNamedIdError(
                    f"Error finding embedded named identifier in case {case.name}: "
                    + f"failed finding identifier named '{imatch['identifier']}' with attribute '{imatch['attribute']}'"
                )
            v = identifier_regex.sub(translated_id, v, count=1)

        return v

    return df.applymap(translate_id)


class UniqueIdGenerator:  # pylint: disable=too-few-public-methods
    """
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
    """

    def __init__(self, fmt=int):
        self.fmt = fmt
        self.size = 1
        self.gen_sample()

    def gen_sample(self):
        self.sample = list(range(10 ** (self.size - 1), 10 ** self.size))
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
    "IdGenerators contain methods that return functions that when called generate identifier values"

    @staticmethod
    def unique_integer():
        return UniqueIdGenerator(str)

    @staticmethod
    def unique_string(prefix=""):
        return UniqueIdGenerator(lambda i: "{}{}".format(prefix, i))

    @staticmethod
    def uuid():
        return lambda: str(uuid.uuid4())


class UnableToFindNamedIdError(Exception):
    pass


class Identifier:
    def __init__(self, attributes, name=None):
        self.attributes = attributes
        self.cached_ids = {}
        self.name = name

        self.generators = {}
        for attr, props in self.attributes.items():
            generator_args = {k: v for k, v in props.items() if k != "generator"}
            self.generators[attr] = getattr(IdGenerators, props["generator"])(
                **generator_args
            )

    def generate(self, case, named_id):
        case_id = id(case)
        if case_id not in self.cached_ids:
            self.cached_ids[case_id] = SimpleNamespace(named_ids={}, case=case)

        if named_id not in self.cached_ids[case_id].named_ids:
            self.cached_ids[case_id].named_ids[named_id] = {}
            for attr, generator in self.generators.items():
                if named_id:
                    value = generator()
                else:
                    value = None
                self.cached_ids[case_id].named_ids[named_id][attr] = value

        return self.cached_ids[case_id].named_ids[named_id]

    def find(self, attribute, raw_id, target_name="Unknown"):
        "Given an attribute and a raw id, return named attribute and case"
        found = SimpleNamespace(named_id=None, case=None)
        for case_name, case in self.cached_ids.items():
            for named_id, attributes in case.named_ids.items():
                if attributes[attribute] == raw_id:
                    found.named_id = named_id
                    found.case = case.case
                    return found

        raise UnableToFindNamedIdError(
            f'In target "{target_name}", unable to find named identifier for value "{raw_id}" '
            + f'belonging to identifier "{self.name}" and attribute "{attribute}"'
        )


def _frame_is_equal(df1, df2):
    try:
        assert_frame_equal(df1, df2)
    except AssertionError:
        return False
    return True


class IdentifierWithoutColumnError(Exception):
    pass


class CannotStackStaticSourceError(Exception):
    pass


class Source:
    def __init__(
        self,
        defaults=None,
        id_mapping=None,
        name=None,
        description=None,
        identifiers=None,
    ):
        self.defaults = defaults
        self.id_mapping = id_mapping
        self.name = name
        self.description = description
        self.identifiers = identifiers or {}
        self.data = pd.DataFrame()

    def stack(self, case, data, values=None):
        "values override defaults at stack time"

        prepped_df = data.copy()
        prepped_df = self._add_defaults(prepped_df, values)
        prepped_df = self._special_values(prepped_df)
        prepped_df = translate_embedded_identifiers(prepped_df, case, self.identifiers)

        if self.id_mapping:
            prepped_df = self._translate_column_identifiers(prepped_df, case)
            self.data = pd.concat([self.data, prepped_df], sort=False).reset_index(
                drop=True
            )
        else:
            if len(self.data) > 0 and not _frame_is_equal(self.data, prepped_df):
                raise CannotStackStaticSourceError(
                    f'In case "{case.name}", attempting to stack data onto source "{self.name}" without identifiers:\n {data}'
                )
            self.data = prepped_df

    def _add_defaults(self, df, values):
        default_values = {**(self.defaults or {}), **(values or {})}

        if self.id_mapping:
            identifier_default_columns = set(self.id_mapping.keys()) - (
                set(default_values.keys()) | set(df.columns)
            )

            for column in identifier_default_columns:
                df[column] = [str(uuid.uuid4()) for _ in range(len(df))]

        for column, value in default_values.items():
            if column in df.columns:
                continue
            df[column] = value

        return df

    def _translate_column_identifiers(self, df, case):
        missing_columns = set(self.id_mapping.keys()) - set(df.columns)
        if len(missing_columns) > 0:
            raise IdentifierWithoutColumnError(
                f'In case "{case.name}", data source "{self.name}" is missing columns corresponding to identifier attributes: {missing_columns}'
            )

        for column, mapto in self.id_mapping.items():
            df[column] = df[column].apply(
                lambda v, mapto=mapto: mapto["identifier"].generate(
                    case=case, named_id=v
                )[mapto["attribute"]]
            )
        return df

    @staticmethod
    def _special_values(df):
        return df.applymap(lambda v: None if v == NULL_TOKEN else v)

    def serialize(self, orient="records"):
        return json.loads(self.data.to_json(orient=orient))


class EmptyDataNoColumnsError(Exception):
    pass


class UnableToFindCaseError(Exception):
    pass


class Target:
    def __init__(self, id_mapping=None, name=None, description=None):
        self.id_mapping = id_mapping or {}
        self.name = name
        self.description = description
        self.data = pd.DataFrame()

    def load_actual(self, records, columns=None):
        """
        Loads actual data into a target.  Used for comparisons with expected.

        Args:
            records (list): A list of dictionaries where each dictionary has keys
                that are the names of the columns in the target.
            columns (list): A list of column names (needed if there is a chance that
                records will be an empty list, e.g., no records)


        Examples:
            Load 2 records into the target::

                mytarget.load_actual([
                    {"id": "1", "name": "Buffy"},
                    {"id": "2", "name": "Willow"},
                ], columns=["id", "name"])
        """

        if len(records) == 0 and len(columns or []) == 0:
            raise EmptyDataNoColumnsError(
                f'Attempting to load target "{self.name}" with 0 records without specifying columns.'
            )

        self.data = pd.DataFrame.from_records(records, columns=columns)

        for column in self.id_mapping.keys():
            if column not in self.data:
                raise KeyError(
                    f'Target "{self.name}" defines identifier map for column "{column}", '
                    f'but "{column}" not found in actual data.  '
                    f"columns found: {list(self.data.columns)}"
                )

        self._translate_special_values()
        self._lookup_case()
        self._translate_column_identifiers()

    def _translate_special_values(self):
        self.data = self.data.applymap(lambda v: NULL_TOKEN if v is None else v)

    def _translate_column_identifiers(self):
        for column, mapto in self.id_mapping.items():

            def _lkp_named_id(v, mapto=mapto):
                if v == NULL_TOKEN:
                    return v
                return (
                    mapto["identifier"]
                    .find(attribute=mapto["attribute"], raw_id=v, target_name=self.name)
                    .named_id
                )

            self.data[column] = self.data[column].apply(_lkp_named_id)

    def _lookup_case(self):
        if len(self.data) == 0:
            self.data["__dtspec_case__"] = pd.Series()
            return

        if len(self.id_mapping) == 0:
            return

        def _lkp_case(row):
            for column, mapto in self.id_mapping.items():
                if row[column] == NULL_TOKEN:
                    continue
                return (
                    mapto["identifier"]
                    .find(
                        attribute=mapto["attribute"],
                        raw_id=row[column],
                        target_name=self.name,
                    )
                    .case
                )
            raise UnableToFindCaseError(
                f'For target "{self.name}", unable to find case for the following record. '
                + f"Perhaps all identifiers null?: {dict(row)}\n"
            )

        self.data["__dtspec_case__"] = self.data.apply(_lkp_case, axis=1)

    def case_data(self, case):
        if "__dtspec_case__" not in self.data.columns:
            return self.data

        return (
            self.data[self.data["__dtspec_case__"].apply(id) == id(case)]
            .drop(columns="__dtspec_case__")
            .reset_index(drop=True)
        )


class Factory:
    def __init__(
        self, data=None, sources=None, inherit_from=None, name=None, description=None
    ):
        self.data = data or {}
        self._parse_tables()
        self._compose_data(inherit_from)
        self.name = name
        self.description = description
        self.sources = sources

    def generate(self, case):
        for source_name in self.data.keys():
            source = self.sources[source_name]
            source.stack(
                case=case,
                data=self.data[source_name]["dataframe"],
                values={
                    **(source.defaults or {}),
                    **self.data[source_name].get("values", {}),
                },
            )

    @staticmethod
    def merge_data(data1, data2):
        data1 = copy.deepcopy(data1)
        data2 = copy.deepcopy(data2)
        merged = {**data1, **data2}
        for source_name in merged.keys():
            if "values" in merged[source_name]:
                merged[source_name]["values"] = {
                    **data1.get(source_name, {}).get("values", {}),
                    **data2.get(source_name, {}).get("values", {}),
                }
        return merged

    def _parse_tables(self):
        for source_name, source_def in self.data.items():
            try:
                self.data[source_name]["dataframe"] = markdown_to_df(
                    source_def["table"]
                )
            except BadMarkdownTableError as err:
                raise BadMarkdownTableError(
                    f"Unable to generate data for source {source_name}:\n{err}"
                )

    def _compose_data(self, inherit_from):
        if inherit_from is None:
            return

        factories = inherit_from + [self]
        composed_data = factories.pop(0).data
        for factory in factories:
            composed_data = self.merge_data(composed_data, factory.data)
        self.data = composed_data


class DuplicateCaseError(Exception):
    pass


class Scenario:  # pylint: disable=too-few-public-methods
    def __init__(self, name=None, cases=None, description=None):
        self.name = name or f"None - {id(self)}"
        self.description = description
        self.cases = cases or {}

    def generate(self):
        for _case_name, case in self.cases.items():
            case.factory.generate(case)


class Case:  # pylint: disable=too-few-public-methods
    def __init__(self, name=None, factory=None, expectations=None, description=None):
        self.name = name or f"None - {id(self)}"
        self.factory = factory
        self.expectations = expectations or []
        self.description = description

    def assert_expectations(self):
        for expectation in self.expectations:
            expectation.load_actual(expectation.target.case_data(self))
            expectation.assert_expected(self)

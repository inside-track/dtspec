import uuid

import pandas as pd
# A collection of sources is the primary output of the data seed stage
# the source will keep track of which records belong to which cases
# every time a case requests data from a source, those records will stack onto the existing records


# what is the relationship between factories and sources?
#   a factory provides data example to a source that then stacks it
#   the factory will include the case that it is being used it
class IdentifierWithoutColumnError(Exception): pass

class Source:
    def __init__(self, defaults=None, id_mapping=None):
        self.defaults = defaults
        self.id_mapping = id_mapping
        self.data = pd.DataFrame()


    def stack(self, case, data, values=None):
        'values override defaults at stack time'

        w_defaults_df = self._add_defaults(data, case, values)
        translated_df = self._translate_identifiers(w_defaults_df, case)
        self.data = pd.concat([self.data, translated_df]).reset_index(drop=True)

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
                'Data source is missing columns corresponding to identifier attributes: {}'.format(missing_columns)
            )


        for column, mapto in self.id_mapping.items():
            df[column] = df[column].apply(
                lambda v: mapto['identifier'].record(case=case, named_id=v)[mapto['attribute']]
            )
        return df

    def to_json(self, orient='records'):
        return self.data.to_json(orient=orient)

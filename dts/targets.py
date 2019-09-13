import pandas as pd

# Targets will be able to hold the result data and get a subset of it for a given case

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

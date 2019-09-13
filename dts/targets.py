import pandas as pd

# Targets will be able to hold the result data and get a subset of it for a given case

class Target:
    def __init__(self, id_mapping):
        self.id_mapping = id_mapping
        self.data = pd.DataFrame()

    def load_actual(self, records):
        self.data = pd.DataFrame.from_records(records)
        self._translate_identifiers()

    def _translate_identifiers(self):
        for column, mapto in self.id_mapping.items():
            lkp = {
                raw_id: mapto['identifier'].find(mapto['attribute'], raw_id)
                for raw_id in self.data[column]
            }
            self.data['__dts_case__'] = self.data[column].apply(lambda v: lkp[v].case)
            self.data[column] = self.data[column].apply(lambda v: lkp[v].named_id)

    def case_data(self, case):
        return (
            self.data[self.data['__dts_case__'] == case]
            .drop(columns='__dts_case__')
            .reset_index(drop=True)
        )

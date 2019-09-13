import pandas as pd

# Targets will be able to hold the result data and get a subset of it for a given case

class Target:
    def __init__(self, id_mapping):
        self.id_mapping = id_mapping
        self.data = pd.DataFrame()

    def load_actual(self, records):
        self.data = pd.DataFrame.from_records(records)
        self._translate_identifiers()

    # TODO: raise an error if a record belongs to more than one case?
    def _translate_identifiers(self):
#         for column, mapto in self.id_mapping.items():
#             self.data[column] = self.data[column].apply(
#                 lambda v: mapto['identifier'].find(named_id=v, attribute=mapto['attribute'])
#             )
# #            self.data['__dts_case__'] = self.data[column].apply()

        pass

    def case_data(self, case):
        pass

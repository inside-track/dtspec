import pandas as pd
import dts.data
# A collection of sources is the primary output of the data seed stage
# the source will keep track of which records belong to which cases
# every time a case requests data from a source, those records will stack onto the existing records


# what is the relationship between factories and sources?
#   a factory provides data example to a source that then stacks it
#   the factory will include the case that it is being used it
class Source:
    def __init__(self, id_mapping=None):
        self.id_mapping = id_mapping
        self.data = pd.DataFrame()


    def stack(self, case, markdown):
        raw_df = dts.data.markdown_to_df(markdown)
        translated_df = self._translate_identifiers(raw_df, case)
        self.data = pd.concat([self.data, translated_df]).reset_index(drop=True)

    def _translate_identifiers(self, df, case):
        for column, mapto in self.id_mapping.items():
            df[column] = df[column].apply(
                lambda v: mapto['identifier'].record(case=case, named_id=v)[mapto['attribute']]
            )
        return df

    def to_json(self, orient='records'):
        return self.data.to_json(orient=orient)

from copy import deepcopy

import dts
import dts.data


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
        data1 = deepcopy(data1)
        data2 = deepcopy(data2)
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
            self.data[source_name]['dataframe'] = dts.data.markdown_to_df(source_def['table'])

    def _compose_data(self, inherit_from):
        if inherit_from is None:
            return

        factories = inherit_from + [self]
        composed_data = factories.pop(0).data
        for factory in factories:
            composed_data = self.merge_data(composed_data, factory.data)
        self.data = composed_data

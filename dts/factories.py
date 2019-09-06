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

    def _parse_tables(self):
        for source_name, source_def in self.data.items():
            self.data[source_name]['dataframe'] = dts.data.markdown_to_df(source_def['table'])

    def _compose_data(self, inherit_from):
        if inherit_from is None:
            return

        factories = inherit_from + [self]
        composed_data = deepcopy(factories.pop(0).data)
        for factory in factories:
            composed_data = {**composed_data, **deepcopy(factory.data)}
        self.data = composed_data

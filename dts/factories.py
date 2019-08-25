import dts
import dts.data

# To test:
#  - factory inheritance - where a latter factory will override/add-to an earlier one
#  - When a factory is "called" in some way, it goes through all of it's data sources and stacks them

class Factory:
    def __init__(self, data=None, sources=None):
        self.data = data
        self.sources = sources

    def generate(self, case):
        for source_name in self.data.keys():
            source = self.sources[source_name]
            source.stack(
                case=case,
                # factories are the only thing that should call sources, so I could refactor
                # sources to accept a dataframe an keep the parsing in Factories
                markdown=self.data[source_name]['table'],
                values={
                    **(source.defaults or {}),
                    **self.data[source_name].get('values', {})
                }
            )

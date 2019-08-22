import dts
import dts.data
import dts.identifiers

# How can I test that a factory is working?
# - create a factory with the defining attributes
# - Generate the factory N=#of cases times and show that the data stacks uniquely
# -

class Factory:
    def __init__(self, name, description='', sources=None):
        self.name = name
        self.description = description
        self.sources = sources


# do sources stack?
# how do we keep track of case relationships as sources stack?
# how do I craft more of a seed 'cause this is getting too complicated

class Source:
    def __init__(self, name, data, values=None, identifiers=None):
        self.name = name
        self.values = values
        self.identifiers = identifiers

        self.data = dts.data.markdown_to_df(data)

import random
import uuid

from types import SimpleNamespace

class UniqueIdGenerator: #pylint: disable=too-few-public-methods
    '''
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
    '''
    def __init__(self, fmt=int):
        self.fmt = fmt
        self.size = 1
        self.gen_sample()

    def gen_sample(self):
        self.sample = list(range(10**(self.size-1), 10**self.size))
        random.shuffle(self.sample)
        self.size += 1

    def __next__(self):
        i = self.sample.pop()
        if len(self.sample) == 0:
            self.gen_sample()
        return self.fmt(i)

    def __call__(self):
        return next(self)


# hrmmm...
class IdGenerators:
    @staticmethod
    def unique_integer():
        return UniqueIdGenerator

    @staticmethod
    def unique_string(prefix=''):
        return lambda: UniqueIdGenerator(lambda i: '{}{}'.format(prefix, i))

class Identifier:
    GENERATOR_TYPES = {
        'unique_integer': UniqueIdGenerator,
        'unique_string': lambda: UniqueIdGenerator('str{}'.format),
        'uuid': lambda: uuid.uuid4
    }

    def __init__(self, attributes):
        self.attributes = attributes
        self.cases = {}

        self.generators = {
            attr: self.GENERATOR_TYPES[props['generator']]()
            for attr, props in self.attributes.items()
        }

    def record(self, case, named_id):
        if case not in self.cases:
            self.cases[case] = SimpleNamespace(records={})
#            self.cases[case] = IdentifierCase(self.generators)

        if named_id not in self.cases[case].records:
            # no, not records
            self.cases[case].records[named_id] = SimpleNamespace(values={})
            for attr, generator in self.generators.items():
                self.cases[case].records[named_id].values[attr] = generator()

        return self.cases[case].records[named_id]

#     def __getitem__(self, case):
#         if case not in self.cases:
#             self.cases[case] = IdentifierCase(self.generators)

#         return self.cases[case]

# class IdentifierCase:
#     def __init__(self, generators):
#         self.generators = generators
#         self.records = {}

#     def __getitem__(self, name):
#         if name not in self.records:
#             self.records[name] = SimpleNamespace(values={})
#             for attr, generator in self.generators.items():
#                 self.records[name].values[attr] = generator()

#         return self.records[name]

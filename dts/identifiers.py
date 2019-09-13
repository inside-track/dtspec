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


class IdGenerators:
    'IdGenerators contain methods that return functions that when called generate identifier values'

    @staticmethod
    def unique_integer():
        return UniqueIdGenerator(str)

    @staticmethod
    def unique_string(prefix=''):
        return UniqueIdGenerator(lambda i: '{}{}'.format(prefix, i))

    @staticmethod
    def uuid():
        return lambda: str(uuid.uuid4())


class Identifier:
    def __init__(self, attributes):
        self.attributes = attributes
        self.cases = {}

        self.generators = {}
        for attr, props in self.attributes.items():
            generator_args = {k:v for k,v in props.items() if k != 'generator'}
            self.generators[attr] = getattr(IdGenerators, props['generator'])(**generator_args)

    # TODO: rename to generate
    def record(self, case, named_id):
        if case not in self.cases:
            self.cases[case] = SimpleNamespace(named_ids={})

        if named_id not in self.cases[case].named_ids:
            self.cases[case].named_ids[named_id] = {}
            for attr, generator in self.generators.items():
                self.cases[case].named_ids[named_id][attr] = generator()

        return self.cases[case].named_ids[named_id]

    def find(self, attribute, raw_id):
        for case_name, case in self.cases.items():
            for named_id, attributes in case.named_ids.items():
                if attributes[attribute] == raw_id:
                    return named_id

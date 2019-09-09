import dts
from dts.factories import Factory
# For now, ignore targets.  They're only used in the final assertions.  Need to focus
# on generating the seed data

# For scenarios, I should be testing:
# - Cases inherit factories from the scenario level, but can be overwritten <- but this may be handeld by the api
# - Collecting all cases within a scenario can stack data sources

class DuplicateCaseError(Exception): pass

class Scenario:
    def __init__(self, factories=None, targets=None, cases=None):
        self.targets = targets or {}
        self.cases = cases or {}

    def generate(self):
        for case_name, case in self.cases.items():
            case.factory.generate(id(case))


# TODO: This might just be a simplenamespace
class Case:
    def __init__(self, factory=None, expected=None):
        self.factory = factory
        self.expected = expected or []

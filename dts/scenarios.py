from types import SimpleNamespace

import dts
import dts.data
from dts.factories import Factory

# For now, ignore targets.  They're only used in the final assertions.  Need to focus
# on generating the seed data

# For scenarios, I should be testing:
# - Cases inherit factories from the scenario level, but can be overwritten <- but this may be handeld by the api
# - Collecting all cases within a scenario can stack data sources

class DuplicateCaseError(Exception): pass

class Scenario:
    def __init__(self, cases=None):
        self.cases = cases or {}

    def generate(self):
        for case_name, case in self.cases.items():
            case.factory.generate(id(case))


class Case:
    def __init__(self, factory=None, expectations=None):
        self.factory = factory
        self.expectations = expectations or []

    def assert_expectations(self):
        for expectation in self.expectations:
            expectation.load_actual(expectation.target.case_data(id(self)))
            expectation.assert_expected()

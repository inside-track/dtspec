# Data Test Studio

API for generating test seed data and performing data transformation assertions.

## Developer setup

    conda create --name dts python=3.6
    source activate dts

    pip install pip-tools
    pip install --ignore-installed -r requirements.txt

## Design Principles

In order to eliminate the need to define data schemas in dts, all data generated and compared
will be done with strings.  It will be up to the user to do type conversions when loading
data into their run systems.

# Genereal principles of a dts test spec:

# A dts test spec describes a set of test *cases*, which are used to define
# data sources and the expected results following some set of data transformations.
# Test *cases* can be grouped into *scenarios* that share some common set of data
# sources or data targets.

TODO: Docs from canonical spec that need to be cleaned up:

A test spec is used to test the behavior of a single execution of a
set of data transformations.  The data transformations may involve
multiple steps with multiple data sources and targets.
With dts, we define all of test source data and expectations up front.  If
there are multiple test cases that use the same source data, then each test
case will "stack" (concantenate) data for that source.

After the test spec is defined, dts will respond will all of the stacked
data sources.  The user will then inject those test data sources into whatever
system they are using to run the data transformations.  They will then run
the transformations and collect all of the resulting target data.

The target data is then loaded back into the dts test api, and data assertions
are run to ensure that the actual result data conforms to the
expected data specified in the test spec.

Factories will represent a collection of test data.
Scenarios are a colletion of test cases that concern some shared topic.
  - The code being tested is expected to run only 1 time for a scenario.
  - One run could be associated with multiple scenarios, up to user to decide
Cases will contain assertions about the results of a run
 - they can also contain additional conditions about data defined in a scenario

Identifiers are used to identify specific records and group them into test cases
Identifiers are shared across all scenarios

Note that dts treats all data as string, but this will be a string that can be converted

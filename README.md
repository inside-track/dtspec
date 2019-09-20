# Data Test Studio

dts is an API for testing data transformations.

## Introduction

Testing data transformations is hard.  So hard that a lot of ETL/ELT
processes have little or (more often) no automated tests.
dts aims to make it easier to write and run tests for very complicated
data transformations typically encountered in ETL/ELT.

With dts, we imagine a data transformation process that takes a set of
data **sources** and transforms them into a set of data **targets**.  dts
is primarily concerned with structured data sources, like Pandas
dataframes or database tables.  A user of dts defines data **factories** that
generate source data, and a set of **expectations** that describe how the data
should look after it's been transformed.

While dts is written in Python, it is intended to be used as more of a
language-agnostic API.  A dts user writes a test **spec**, which is then passed
to dts.  dts processes that spec and then returns to the user test data for
all of the source specific in the spec.  The user then feeds that test data into
their data transformation system, collects the output, and sends it back to dts.
dts compares the actual results of the data transformations with the expected
results specific in the spec and reports on any discrepancies.


## Tutorial

Let's see this all at work with some examples.


### Hello World!

Let's suppose we have a dataset containing student records.  Our data
transformation simply reads in that data, and returns a new dataframe
with a "Hello <student>" salutation.  We want to test that it says
"hello" to everyone.  For the purposes of our tutorial, the data
transformation will be written in Pandas as

````python
def hello_world_transformer(raw_students):
    salutations_df = raw_students.copy()
    salutations_df["salutation"] = salutations_df['name'].apply(lambda v: 'Hello ' + v)

    return {"salutations": salutations_df}

````

dts is an API that accepts a JSON blob for the test spec.  However, I strongly
prefer to write specs in YAML and then convert them into JSON before passing them
on to dts.  To begin writing our test spec, we define the dts `version`, a `description`
of the test spec, and then list out the `sources` and `targets`:

````yaml
---
version: '0.1'
description: HelloWorld - Simplest example of running dts

# The names of sources and targets is arbitrary, but it's up to the user to determine
# how they get mapped to/from their data transformation system.
sources:
  - source: raw_students

targets:
  - target: salutations
````

These define our inputs and outputs.  But we also need to define how to generate
data for the input(s).  For that, we define a **factory**:

````yaml
factories:
  - factory: SomeStudents
    description: Minimal example of what some student records may look like

    data:
      - source: raw_students
        # Tables written as a markdown table
        table: |
          | id | name   |
          | -  | -      |
          | 1  | Buffy  |
          | 2  | Willow |
````

Lastly, we need to describe how we expect the data to look after it has been transformed.
To do this, we define **scenarios** and **cases**.  Scenarios are collections of cases
that share some common data factory or describe similar situations.  For now, our
test spec will just contain a single scenario and a single case:

````yaml
scenarios:
  - scenario: Hello World
    description: The simplest scenario
    # All cases in this scenario will use this factory (which may be modified on case-by-case basis)
    factories:
      - SomeStudents

    cases:
      - case: HelloGang
        description: Make sure we say hello to everyone
        expected:
          data:
            - target: salutations
              # The actual output may also contain the "name" field, but the expectation
              # will ignore comparing any fields not listed in the expected table.
              table: |
                | id | salutation   |
                | -  | -            |
                | 1  | Hello Buffy  |
                | 2  | Hello Willow |
````

That's it. See also the [full YAML spec](tests/hello_world.yml).

Now that we've described the full test spec, we need to use it.  The first step is to
parse the YAML file, send it to the dts api, and have dts generate source data:

````python
import dts
import yaml

spec = yaml.safe_load(open("hello_world.yml"))
api = dts.api.Api(spec)
api.generate_sources()
````

The specific steps taken at this point are going to be sensitive to the data transformation
environment being used, but we'll stick with our Pandas transformations for the sake of this
tutorial.  Given this, we can define a simple function that converts the source data returned
from dts into Pandas dataframes:

````python
import pandas as pd

def parse_sources(sources):
    "Converts test data returned from dts api into Pandas dataframes"

    return {
        source_name: pd.DataFrame.from_records(data.serialize())
        for source_name, data in sources.items()
    }
````

We can then run those test Pandas dataframes through our data transformation function.

````python
sources_data = parse_sources(api.spec["sources"])
actual_data = hello_world_transformer(**sources_data)
````

Next, we need to convert the output dataframes of the transformations, `actual_data`,
back into a format that can be loaded into dts for comparison.  For Pandas,
this function is:

````python
def serialize_actuals(actuals):
    "Converts Pandas dataframe results into form needed to load dts api actuals"

    return {
        target_name: json.loads(dataframe.astype(str).to_json(orient="records"))
        for target_name, dataframe in actuals.items()
    }
````

It is loaded into dts using:

````python
serialized_actuals = serialize_actuals(actual_data)
api.load_actuals(serialized_actuals)
````

Finally, dts can be called to run all of the expectations:

````python
api.assert_expectations()
````

Putting all of this together:
````python
spec = yaml.safe_load(open("hello_world.yml"))
api = dts.api.Api(spec)
api.generate_sources()

sources_data = parse_sources(api.spec["sources"])
actual_data = hello_world_transformer(**sources_data)
serialized_actuals = serialize_actuals(actual_data)
api.load_actuals(serialized_actuals)
````

Try running the above code and changing either the YAML spec or the `hello_world_transformer`
function and see how dts responds.

### Hello World With Multiple Test Cases

Running tests with multiple cases that reference the same data sources
introduces a complicating factor. One of the major reasons that makes
it hard to build tests for for ETL/ELT is the fact that many data
transformation systems in use today have a high latency for even very
small transformations.  For example, Redshift is a distributed RDBMS
that can process billions of rows in minutes, millions of rows in
seconds, thousands of rows in seconds, or 10s of rows in, well,
seconds.  Given these latency issues, we don't want to have to rely on
loading data into our system, running a test, clearing out the data,
loading some more, running the next test, and so on as is often
done when testing ORM-based applications like Rails or Django.

dts seeks to minimize the number of requests on the data
transformation system in order to deal with these latency issues.
It does this by "stacking" the test data generated in each case
and delivering back to the user all of this stacked data.  The user
then loads this stacked data into their data transformation system
**once**, runs the data transformations **once**, and then collects
the resulting output **once**.


### A More Realistic Example



## Additional notes about dts


All comparisons are done with strings.  Up to the user to enforce data types
suitable to their data transformation system.

A dts test spec describes a set of test *cases*, which are used to define
data sources and the expected results following some set of data transformations.
Test *cases* can be grouped into *scenarios* that share some common set of data
sources or data targets.

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


## Contributing

### Developer setup

    conda create --name dts python=3.6
    source activate dts

    pip install pip-tools
    pip install --ignore-installed -r requirements.txt

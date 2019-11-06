# Data Transform Spec

dtspec is an API for specifying and testing data transformations.

## Introduction

Testing data transformations is hard.  So hard that a lot of ETL/ELT
processes have little or (more often) no automated tests.
dtspec aims to make it easier to write and run tests for very complicated
data transformations typically encountered in ETL/ELT.

With dtspec, we imagine a data transformation process that takes a set of
data **sources** and transforms them into a set of data **targets**.  dtspec
is primarily concerned with structured data sources, like Pandas
dataframes or database tables.  A user of dtspec defines data **factories** that
generate source data, and a set of **expectations** that describe how the data
should look after it's been transformed.

While dtspec is written in Python, it is intended to be used as more of a
language-agnostic API.  A dtspec user writes a test **spec**, which is then passed
to dtspec.  dtspec processes that spec and then returns to the user test data for
all of the source specific in the spec.  The user then feeds that test data into
their data transformation system, collects the output, and sends it back to dtspec.
dtspec compares the actual results of the data transformations with the expected
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

dtspec is an API that accepts a JSON blob for the transformation spec.  However, I strongly
prefer to write specs in YAML and then convert them into JSON before passing them
on to dtspec.  To begin writing our transform spec, we define the dtspec `version`, a `description`
of the transform spec, and then list out the `sources` and `targets`:

````yaml
---
version: '0.1'
description: HelloWorld - Simplest example of running dtspec

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
transform spec will just contain a single scenario and a single case:

````yaml
scenarios:
  - scenario: Hello World
    description: The simplest scenario
    # All cases in this scenario will use this factory (which may be modified on case-by-case basis)
    factory:
        parents:
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

Now that we've described the full transform spec, we need to use it.  The first step is to
parse the YAML file, send it to the dtspec api, and have dtspec generate source data:

````python
import dtspec
import yaml

spec = yaml.safe_load(open("tests/hello_world.yml"))
api = dtspec.api.Api(spec)
api.generate_sources()
````

The specific steps taken at this point are going to be sensitive to the data transformation
environment being used, but we'll stick with our Pandas transformations for the sake of this
tutorial.  Given this, we can define a simple function that converts the source data returned
from dtspec into Pandas dataframes:

````python
import pandas as pd

def parse_sources(sources):
    "Converts test data returned from dtspec api into Pandas dataframes"

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
back into a format that can be loaded into dtspec for comparison.  For Pandas,
this function is:

````python
def serialize_actuals(actuals):
    "Converts Pandas dataframe results into form needed to load dtspec api actuals"

    return {
        target_name: json.loads(dataframe.astype(str).to_json(orient="records"))
        for target_name, dataframe in actuals.items()
    }
````

It is loaded into dtspec using:

````python
serialized_actuals = serialize_actuals(actual_data)
api.load_actuals(serialized_actuals)
````

Finally, dtspec can be called to run all of the expectations:

````python
api.assert_expectations()
````

Putting all of this together:
````python
spec = yaml.safe_load(open("tests/hello_world.yml"))
api = dtspec.api.Api(spec)
api.generate_sources()

sources_data = parse_sources(api.spec["sources"])
actual_data = hello_world_transformer(**sources_data)
serialized_actuals = serialize_actuals(actual_data)
api.load_actuals(serialized_actuals)
````

Try running the above code and changing either the YAML spec or the `hello_world_transformer`
function and see how dtspec responds.

### Hello World With Multiple Test Cases

Running tests with multiple cases that reference the same data sources
introduces a complicating factor. One of the reasons that makes
it hard to build tests for ETL/ELT is the fact that many data
transformation systems in use today have a high latency for even very
small transformations.  For example, Redshift is a distributed RDBMS
that can process billions of rows in minutes, millions of rows in
seconds, thousands of rows in seconds, or 10s of rows in, well,
seconds.  Given these latency issues, we don't want to have to rely on
loading data into our system, running a test, clearing out the data,
loading some more, running the next test, and so on as is often
done when testing ORM-based applications like Rails or Django.

dtspec seeks to minimize the number of requests on the data
transformation system in order to deal with these latency issues.
It does this by "stacking" the test data generated in each case
and delivering back to the user all of this stacked data.  The user
then loads this stacked data into their data transformation system
**once**, runs the data transformations **once**, and then collects
the resulting output **once**.

Let's see how dtspec handles this in action.

First, let's change our hello world data transformation a bit.  Instead of
just saying hello to our heroes, let's say goodbye to any villians (as
identified by a `clique` data field).

````python
def hello_world_multiple_transformer(raw_students):
    def salutation(row):
        if row["clique"] == "Scooby Gang":
            return "Hello {}".format(row["name"])
        return "Goodbye {}".format(row["name"])

    salutations_df = raw_students.copy()
    salutations_df["salutation"] = salutations_df.apply(salutation, axis=1)

    return {"salutations": salutations_df}
````

While it would be possible to test saying hello or goodbye in a single
case just by adding more records to the source data, we'll split it
into two to demonstrate how multiple cases work.  Here's how the YAML would look:

````yaml
scenarios:
  - scenario: Hello World With Multiple Cases
    description: The simplest scenario
    factory:
      parents:
        - SomeStudents

    cases:
      - case: HelloGang
        description: Make sure we say hello to everyone
        expected:
          data:
            - target: salutations
              table: |
                | id | name   | clique      | salutation   |
                | -  | -      | -           | -            |
                | 1  | Buffy  | Scooby Gang | Hello Buffy  |
                | 2  | Willow | Scooby Gang | Hello Willow |

      - case: GoodbyeVillians
        description: Say goodbye to villians
        # For this case, we tweak the factory defined for the scenario.
        factory:
          # The ids here might be the same as above.  However, these are just named
          # references and get translated into unique ids when the source data
          # is generated.
          data:
            - source: raw_students
              table: |
                | id | name     |
                | -  | -        |
                | 1  | Drusilla |
                | 2  | Harmony  |
              # Use values to populate a constant over all records
              values:
                - column: clique
                  value: Vampires

        expected:
          data:
            # Again, the ids here are not the actual ids sent to dtspec after performing
            # the transformations.  They are just named references and dtspec
            # keeps track of the relationship between the actual ids and the named ones.
            - target: salutations
              table: |
                | id | name     | clique   | salutation       |
                | -  | -        | -        | -                |
                | 1  | Drusilla | Vampires | Goodbye Drusilla |
                | 2  | Harmony  | Vampires | Goodbye Harmony  |

````

This won't quite work as is, because we're missing something.  We have
two cases that describe variations on the source data `raw_students`
and the output `salutations`.  dtspec collects the source data
definitions from each case and stacks them into a single data source.
The user then runs the transformations on that source and generates a
single target to provide back to dtspec.  But dtspec has to know which record
belongs to which case.  To do this, we have to define an
**identifier** that tells dtspec which columns should be used to identify
a record as belonging to a case.  A good identifier is often a primary
key that uniquely defines a record, but it is not strictly required to
be unique across all records.

For this example, we'll define an identifier called "students" with a single
**identifier attribute** called `id` that is a unique integer:

````yaml
identifiers:
  - identifier: students
    attributes:
      - field: id
        generator: unique_integer
````

We tell dtspec that this identifier is associated with the `id` columns of both
the source and the target via:

````yaml
sources:
  - source: raw_students
    identifier_map:
      - column: id
        identifier:
          name: students
          attribute: id


targets:
  - target: salutations
    identifier_map:
      - column: id
        identifier:
          name: students
          attribute: id
````

With the sources and targets with identifiers, the values we see in
the source factories and target expectations are not the values that
are actually used in the data.  Instead, they are simply **named
refereces**.  For example, in the "HelloGang" case, `id=1` belongs to
Buffy and `id=2` belongs to Willow.  But when dtspec generates the source
data, the actual values may be 3 and 9, or 4 and 7, or something else.
Unique values are not generated in any deterministic manner -- each
run of dtspec can give a diferent set.  dtspec only guarantees that the
each named reference will be a unique integer (via the `generator`
defined in the `identifier` section).

Futhermore, in the second case called "GoodbyeVillians", we see that
`id=1` belongs to Drusilla and `id=2` belongs to Harmony.  dtspec will
generate unique values for this case as well, and they **will not**
conflict with the values generated for the first case.  So dtspec will pass
back to the user 4 total records (Buffy, Willow, Drusilla, Harmony) with 4
different ids

With the [full YAML spec](tests/hello_world_multiple_cases.yml) defined, we can
run the assertions in the same fashion as the the earlier example

````python
spec = yaml.safe_load(open("tests/hello_world_multiple_cases.yml"))
api = dtspec.api.Api(spec)
api.generate_sources()

sources_data = parse_sources(api.spec["sources"])
actual_data = hello_world_multiple_transformer(**sources_data)
serialized_actuals = serialize_actuals(actual_data)
api.load_actuals(serialized_actuals)

api.assert_expectations()
````

#### Embedded Identifiers

It is also possible to embed identifiers in the value of a particular column.
For example, suppose our `salutation` column said hello to the `id` instead
of the name of the person.  To make this work, we have to put a particular
string pattern in the column that indicates the name of the identifier, the
attribute, and the named id - `{identifier.attribute[named_id]}`.  The
yaml spec would look like:

````yaml
      - case: HelloGang
        description: Make sure we say hello to everyone
        expected:
          data:
            - target: salutations
              table: |
                | id | name   | clique      | salutation             |
                | -  | -      | -           | -                      |
                | 1  | Buffy  | Scooby Gang | Hello {students.id[1]} |
                | 2  | Willow | Scooby Gang | Hello {students.id[2]} |
````
The [realistic example](tests/realistic.yml) discussed below has another example
of using embedded identifiers.

**Note** that embedded identifiers cannot be used to associate records
with cases.  A target must have at least one column listed in the
`identifier_map` section.

### A More Realistic Example

Finally, let's example a more realistic example that one might
encounter when building a data warehouse.  In these situations, we'll
have multiple sources, targets, scenarios, and cases.  Now suppose we
have a students table, where every student belongs to a school and
takes 0 to many classes.  Our goal is to create one denormalized table
that combines all of these data sources into one table.  Additionally,
we want to create a table that aggregates all of our students to give
a count of the students per school.  In Pandas, the data transformation
might look like:

````python
def realistic_transformer(raw_students, raw_schools, raw_classes, dim_date):

    student_schools = raw_students.rename(
        columns={"id": "student_id", "external_id": "card_id"}
    ).merge(
        raw_schools.rename(columns={"id": "school_id", "name": "school_name"}),
        how="inner",
        on="school_id",
    )

    student_classes = student_schools.merge(
        raw_classes.rename(columns={"name": "class_name"}),
        how="inner",
        on="student_id",
    ).merge(
        dim_date.rename(columns={"date": "start_date"}), how="left", on="start_date"
    )

    student_classes["student_class_id"] = student_classes.apply(
        lambda row: "-".join([str(row["card_id"]), str(row["class_name"])]), axis=1
    )

    students_per_school = (
        student_schools.groupby(["school_name"])
        .size()
        .to_frame(name="number_of_students")
        .reset_index()
    )

    return {
        "student_classes": student_classes,
        "students_per_school": students_per_school,
    }
````

Given the [full YAML spec](tests/realistic.yml) defined, we can again run
the data assertions using a familiar pattern:

````python
spec = yaml.safe_load(open("tests/realistic.yml"))
api = dtspec.api.Api(spec)
api.generate_sources()

sources_data = parse_sources(api.spec["sources"])
actual_data = hello_world_multiple_transformer(**sources_data)
serialized_actuals = serialize_actuals(actual_data)
api.load_actuals(serialized_actuals)

api.assert_expectations()
````



## Additional notes about dtspec

* At the moment, all source data values are generated as strings.  It
  is up to the the user to enforce data types suitable to their data
  transformation system.
* Additionally, data expectations are stringified prior to running assertions.

## Contributing

We welcome contributors!  Please submit any suggests or pull requests in Github.

### Developer setup

Create an appropriate python environment.  I like [miniconda](https://conda.io/miniconda.html),
but use whatever you like:

    conda create --name dtspec python=3.6
    source activate dtspec

Then install pip packages

    pip install pip-tools
    pip install --ignore-installed -r requirements.txt

run tests via

    inv test

and the linter via

    inv lint

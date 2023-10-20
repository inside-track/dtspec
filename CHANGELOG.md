# dtspec Changelog

## 0.7.6

* dependencies version bumps

## 0.7.5

* Resolves an issue loading Snowflake with empty data

## 0.7.4

* Resolve issue with cleaning up databases with Snowflake.
* Allows dtspec to compile dbt snapshots as valid refs

## 0.7.3

* Resolves issue with Snowflake authentication options when using username/password
* Creates test target schema if not exists prior to checking for table/view existence
* Serialize special SQLAlchemy values prior to loading

## 0.7.2

* Resolve confusing `project_dir` that should have been `profile_dir` CLI argument (@mdesmit)
* Instead of raising an error, will drop targets that are modeled as views
* Support for snowflake private key authentication (@mdesmit)
* CLI Integration tests using dbt-container-skeleton project
* Using github actions for CI

## 0.7.0

* New `dtspec` cli command that helps with setting up test environments and running against dbt projects.

## 0.6.0

* Identifiers can now be embedded in both source and expectations.

## 0.5.0

* Null identifiers can now be in targets, as long as one column can be used to identify
  a record as belonging to a case.

## 0.4.0

* [breaking] Loading actuals now expect a "columns" field to be present.  This
  helps with handling empty data.

## 0.3.0

* Name of this project is now "Data Transform Spec".
* Rudamentary method to convert spec into a markdown document.
* Can now use "values" to specify constants in target data expectations.
* Improved user-facing error messaging.


## 0.2.0

* Identifiers can now be null.
* When data is serialized/deserialized, "{NULL}" string is treated as null/None.
* Improved user-facing error messaging.


## 0.1.1

* Adds a `compare_via` option for data expectations.

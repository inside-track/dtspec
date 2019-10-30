# dtspec Changelog

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

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

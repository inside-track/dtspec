import jsonschema

import dts
from dts.identifiers import Identifier
from dts.factories import Factory
from dts.sources import Source
from dts.targets import Target
from dts.scenarios import Scenario, Case
from dts.expectations import DataExpectation

SCHEMA={
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Data Test Studio API spec",
    "description": "Data Test Studio API spec",
    "definitions": {
        "identifier_map": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["column", "identifier"],
                "additionalProperties": False,
                "properties": {
                    "column": {"type": "string"},
                    "identifier": {
                        "type": "object",
                        "required": ["name", "attribute"],
                        "additionalProperties": False,
                        "properties": {
                            "name": {"type": "string"},
                            "attribute": {"type": "string"}
                        }
                    }
                }
            }
        },
        "factory_data": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["source", "table"],
                "additionalProperties": False,
                "properties": {
                    "source": {"type": "string"},
                    "table": {"type": "string"},
                    "values": {"$ref": "#/definitions/column_values"},
                }
            }
        },
        "column_values": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["column", "value"],
                "additionalProperties": False,
                "properties": {
                    "column": {"type": "string"},
                    "value": {"type": "string"}
                }
            }
        },
        "expected": {
            "type": "object",
            "required": ["data"],
            "additionalProperties": False,
            "properties": {
                "data": {
                    "type": "array",
                    "minItems": 1,
                    "uniqueItems": True,
                    "items": {
                        "type": "object",
                        "required": ["target"],
                        "additionalProperties": False,
                        "properties": {
                            "target": {"type": "string"},
                            "table": {"type": "string"},
                            "by": {"type": "array", "items": {"type": "string"}}
                        },
                    },
                },
            },
        },
    },
    "type": "object",
    "properties": {
        "version": {"type": "string"},
        "identifiers": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["identifier", "attributes"],
                "additionalProperties": False,
                "properties": {
                    "identifier": {"type": "string"},
                    "attributes": {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": True,
                        "items": {
                            "type": "object",
                            "required": ["field", "generator"],
                            "additionalProperties": True,
                            "properties": {
                                "field": {"type": "string"},
                                "generator": {"type": "string"}
                            },
                        },
                    },
                },
            },
        },
        "sources": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["source"],
                "addtionalProperties": False,
                "properties": {
                    "source": {"type": "string"},
                    "defaults": {"$ref": "#/definitions/column_values"},
                    "identifier_map": {"$ref": "#/definitions/identifier_map"}
                },
            },
        },
        "targets": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["target"],
                "addtionalProperties": False,
                "properties": {
                    "target": {"type": "string"},
                    "identifier_map": {"$ref": "#/definitions/identifier_map"}
                },
            },
        },
        "factories": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["factory", "data"],
                "additionalProperties": False,
                "properties": {
                    "factory": {"type": "string"},
                    "description": {"type": "string"},
                    "parents": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "data": {"$ref": "#/definitions/factory_data"}
                },
            },
        },
        "scenarios": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["scenario", "cases"],
                "additionalProperties": False,
                "properties": {
                    "scenario": {"type": "string"},
                    "description": {"type": "string"},
                    "factories": {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": True,
                        "items": {"type": "string"}, # TODO: extend this to allow anonymous factories too
                    },
                    "cases": {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": True,
                        "items": {
                            "type": "object",
                            "required": ["case", "expected"],
                            "additionalProperties": False,
                            "properties": {
                                "case": {"type": "string"},
                                "description": {"type": "string"},
                                "factory": {
                                    "type": "object",
                                    "required": ["data"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "data": {"$ref": "#/definitions/factory_data"}
                                    }
                                },
                                "expected": {"$ref": "#/definitions/expected"}
                            },
                        },
                    },
                },
            },
        },
        "metadata": { "type": "object" }
    },
    "required": ["version", "sources", "scenarios"],
    "additionalProperties": False
}

class ApiValidationError(Exception): pass
class ApiDuplicateError(ApiValidationError): pass
class ApiReferentialError(ApiValidationError): pass

# TODO: descriptions on all the things
class Api:
    def __init__(self, json_spec):
        self.spec = {}
        self._parse_spec(json_spec)

    def _parse_spec(self, json_spec):
        'Converts the raw JSON spec into internal objects used to generate source data and run assertions'
        jsonschema.validate(json_spec, SCHEMA)

        self._parse_spec_identifiers(json_spec)
        self._parse_spec_sources(json_spec)
        self._parse_spec_targets(json_spec)
        self._parse_spec_factories(json_spec)
        self._parse_spec_scenarios(json_spec)


    def generate_sources(self):
        'Used to generate all source data that will be passed back to the user'
        for _scenario_name, scenario in self.spec['scenarios'].items():
            scenario.generate()


    def source_data(self):
        return {name: source.serialize() for name, source in self.spec['sources'].items()}

    def load_actuals(self, actuals_json):
        '''
        Used to load data containing the actual results to be compared with expectations

        Expecting a json object with keys that are the names of data targets
        and values that are an array of records.  Each record is a json object
        with keys that are column names and values that are the record column values.
        '''

        for target, records in actuals_json.items():
            self.spec['targets'][target].load_actual(records)

    def run_assertions(self):
        'Runs all of the assertions defined in the spec against the actual data'
        for scenario_name, scenario in self.spec['scenarios'].items():
            for case_name, case in scenario.cases.items():
                case.assert_expectations()

    def _parse_spec_identifiers(self, json_spec):
        self.spec['identifiers'] = {}

        for identifier_json in json_spec.get('identifiers', []):
            identifier_name = identifier_json['identifier']
            attr_list = identifier_json['attributes']

            if identifier_name in self.spec['identifiers']:
                raise ApiDuplicateError(f'Duplicate identifiers detected: {identifier_name}')

            attributes = {}
            for attr in attr_list:
                attr_name = attr['field']
                attr_args = {k:v for k,v in attr.items() if k != 'field'}
                attributes[attr_name] = attr_args

            self.spec['identifiers'][identifier_name] = Identifier(attributes)


    def _parse_spec_sources(self, json_spec):
        self.spec['sources'] = {}
        for source_json in json_spec['sources']:
            source_name = source_json['source']

            defaults = {}
            for default in source_json.get('defaults', {}):
                defaults[default['column']] = default['value']

            id_mapping = self._parse_identifier_map(source_json.get('identifier_map', []), 'source', source_name)

            self.spec['sources'][source_name] = Source(
                defaults=defaults,
                id_mapping=id_mapping
            )

    def _parse_identifier_map(self, map_json, data_type=None, data_name=None):
        id_mapping = {}
        for id_map in map_json:
            identifier_name = id_map['identifier']['name']
            if identifier_name not in self.spec['identifiers']:
                raise ApiReferentialError(
                    f'Unable to find identifier "{identifier_name}" referenced in {data_type}: "{data_name}"'
                )
            id_mapping[id_map['column']] = {
                'identifier': self.spec['identifiers'][identifier_name],
                'attribute': id_map['identifier']['attribute']
            }
        return id_mapping


    def _parse_spec_targets(self, json_spec):
        self.spec['targets'] = {}
        for target_json in json_spec['targets']:
            target_name = target_json['target']
            id_mapping = self._parse_identifier_map(target_json.get('identifier_map', []), 'target', target_name)

            self.spec['targets'][target_name] = Target(id_mapping=id_mapping)


    def _parse_spec_factories(self, json_spec):
        self.spec['factories'] = {}
        for factory_json in json_spec.get('factories', []):
            factory_name = factory_json['factory']
            factory_data = self._parse_spec_factory_data(factory_json['data'], factory_name)

            if factory_name in self.spec['factories'].keys():
                raise ApiDuplicateError(f'Duplicate factories detected: {factory_name}')


            inherit_from = []
            for parent_factory in factory_json.get('parents', []):
                if parent_factory not in self.spec['factories'].keys():
                    raise ApiReferentialError(
                        f'Unable to find parent factory "{parent_factory}" referenced in factory "{factory_name}"'
                    )

                inherit_from.append(self.spec['factories'][parent_factory])

            self.spec['factories'][factory_name] = Factory(
                data=factory_data,
                inherit_from=inherit_from,
                sources=self.spec['sources']
            )

    def _parse_spec_factory_data(self, json_spec, factory_name):
        spec = {}
        for data_json in json_spec:
            source_name = data_json['source']
            if source_name not in self.spec['sources'].keys():
                raise ApiReferentialError(
                    f'Unable to find source "{source_name}" referenced in factory "{factory_name}"'
                )
            spec[source_name] = {
                'table': data_json.get('table', None),
                'values': {value['column']: value['value'] for value in data_json.get('values', [])}
            }

        return spec

    def _parse_spec_scenarios(self, json_spec):
        self.spec['scenarios'] = {}
        for scenario_json in json_spec['scenarios']:
            scenario_name = scenario_json['scenario']

            if scenario_name in self.spec['scenarios'].keys():
                raise ApiDuplicateError(f'Duplicate scenarios detected: {scenario_name}')

            self.spec['scenarios'][scenario_name] = Scenario(
                cases=self._parse_spec_cases(
                    scenario_json['cases'],
                    scenario_json.get('factories', []),
                    scenario_name
                )
            )

    def _parse_spec_cases(self, cases_json, scenario_factories, scenario_name):
        cases = {}
        for case_json in cases_json:
            case_name = case_json['case']
            if case_name in cases:
                raise ApiDuplicateError(f'Duplicate cases detected in scenario "{scenario_name}": {case_name}')

            case_data = self._parse_spec_factory_data(
                case_json.get('factory', {}).get('data', []),
                f'Case Factory: {case_name}'
            )
            cases[case_name] = Case(
                factory=Factory(
                    sources=self.spec['sources'],
                    inherit_from=[self.spec['factories'][name] for name in scenario_factories],
                    data=case_data
                ),
                expectations=self._parse_spec_expectations(case_json['expected'])
            )
        return cases


    def _parse_spec_expectations(self, expectations_json):
        expectations = []
        for expected_data in expectations_json['data']:
            target_name = expected_data['target']
            if target_name not in self.spec['targets']:
                raise ApiReferentialError(
                    f'Unable to find target "{target_name}" referenced in expectation'
                )

            target = self.spec['targets'][target_name]
            expectations.append(
                DataExpectation(
                    target=target,
                    table=expected_data['table'],
                    by=expected_data.get('by', [])
                )
            )
        return expectations

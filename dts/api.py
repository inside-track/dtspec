import jsonschema

import dts
from dts.identifiers import Identifier
from dts.factories import Factory
from dts.sources import Source
from dts.scenarios import Scenario, Case

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

        if 'identifiers' in json_spec:
            self.spec['identifiers'] = self._parse_spec_identifiers(json_spec['identifiers'])

        if 'sources' in json_spec:
            self.spec['sources'] = self._parse_spec_sources(json_spec['sources'])

        if 'targets' in json_spec:
            self.spec['targets'] = self._parse_spec_targets(json_spec['targets'])

        if 'factories' in json_spec:
            self.spec['factories'] = self._parse_spec_factories(json_spec['factories'])


    def generate_sources(self):
        'Used to generate all source data that will be passed back to the user'
        raise NotImplementedError

    def load_actuals(self):
        'Used to load data containing the actual results to be compared with expectations'
        raise NotImplementedError

    def run_assertions(self):
        'Runs all of the assertions defined in the spec against the actual data'
        raise NotImplementedError

    @staticmethod
    def _parse_spec_identifiers(json_spec):
        spec = {}
        for identifier_json in json_spec:
            identifier_name = identifier_json['identifier']
            attr_list = identifier_json['attributes']

            if identifier_name in spec:
                raise ApiDuplicateError(f'Duplicate identifiers detected: {identifier_name}')

            attributes = {}
            for attr in attr_list:
                attr_name = attr['field']
                attr_args = {k:v for k,v in attr.items() if k != 'field'}
                attributes[attr_name] = attr_args

            spec[identifier_name] = Identifier(attributes)
        return spec

    def _parse_spec_sources(self, json_spec):
        spec = {}
        for source_json in json_spec:
            source_name = source_json['source']

            defaults = {}
            for default in source_json.get('defaults', {}):
                defaults[default['column']] = default['value']

            id_mapping = {}
            for id_map in source_json.get('identifier_map', {}):
                identifier_name = id_map['identifier']['name']
                if identifier_name not in self.spec['identifiers']:
                    raise ApiReferentialError(
                        f'Unable to find identifier "{identifier_name}" referenced in source "{source_name}"'
                    )
                id_mapping[id_map['column']] = {
                    'identifier': self.spec['identifiers'][identifier_name],
                    'attribute': id_map['identifier']['attribute']
                }


            spec[source_name] = Source(
                defaults=defaults,
                id_mapping=id_mapping
            )
        return spec

    def _parse_spec_targets(self, json_spec):
        'not yet implemented'
        return {}

    def _parse_spec_factories(self, json_spec):
        spec = {}
        for factory_json in json_spec:
            factory_name = factory_json['factory']
            factory_data = self._parse_spec_factory_data(factory_json['data'], factory_name)

            if factory_name in spec:
                raise ApiDuplicateError(f'Duplicate factories detected: {factory_name}')


            inherit_from = []
            for parent_factory in factory_json.get('parents', []):
                if parent_factory not in spec.keys():
                    raise ApiReferentialError(
                        f'Unable to find parent factory "{parent_factory}" referenced in factory "{factory_name}"'
                    )

                inherit_from.append(spec[parent_factory])

            spec[factory_name] = Factory(
                data=factory_data,
                inherit_from=inherit_from,
                sources=self.spec['sources']
            )
        return spec

    def _parse_spec_factory_data(self, json_spec, factory_name):
        spec = {}
        for data in json_spec:
            source_name = data['source']
            if source_name not in self.spec['sources'].keys():
                raise ApiReferentialError(
                    f'Unable to find source "{source_name}" referenced in factory "{factory_name}"'
                )
            spec[source_name] = {
                'table': data.get('table', None),
                'values': {value['column']: value['value'] for value in data.get('values', [])}
            }

        return spec

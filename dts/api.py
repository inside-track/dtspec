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
                "required": ["source"],
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
                        }
                    }
                }
            }

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
                            }
                        }
                    },
                },
            }
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
                }
            }
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
                }
            }
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
                }
            }
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
                            }
                        }
                    }
                }
            }
        },
        "metadata": { "type": "object" }
    },
    "required": ["version", "sources", "scenarios"],
    "additionalProperties": False
}

class Api:
    def __init__(self, json_spec):
        self.spec = self._parse_sepc(json_spec)

    def parse_spec(self, json_spec):
        'Converts the raw JSON spec into internal objects used to generate source data and run assertions'
        jsonschema.validate(json_spec, SCHEMA)

        if 'identifiers' in json_spec:
            self.spec['identifiers'] = self._parse_spec_identifiers(json_spec['identifiers'])

        if 'sources' in json_spec:
            self.spec['sources'] = self._parse_spec_sources(json_spec['sources'])


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
    def _parse_spec_identifiers(raw_spec_identifiers):
        spec_identifiers = {}
        for identifier_name, attributes in raw_spec_identifiers.items():
            spec_identifiers[identifier_name] = Identifier(attributes)
        return spec_identifiers

    @staticmethod
    def _parse_spec_sources(raw_spec_sources):
        spec_sources = {}
        for source_name, attributes in raw_spec_sources.items():
            spec_sources[source_name] = Source(
                defaults=attributes['defaults'],
                id_mapping={
                    col: {
                        'identfier': self.spec['identifiers'][identifier_spec['identifier']],
                        'attribute': identifier_spec['attribute']
                    }
                    for col, identifier_spec in attributes['identifiers'].items()
                }
            )

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
                    "values": {
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
                    }
                }
            }
        }

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
                    "defaults": {
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
                                }
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

class InvalidSpecificationError(Exception): pass
class _Api:
    def __init__(self):
        self.spec = {}

    @staticmethod
    def _validate_spec(spec):
        unknown_keys = set(spec.keys()) - {
            'version', 'identifiers', 'sources', 'targets', 'factories', 'scenarios', 'metadata'
        }
        if len(unknown_keys) > 0:
            raise InvalidSpecificationError('Unknown keys found in test spec: {unknown_keys}')

        if 'version' not in spec:
            raise InvalidSpecificationError('Required key "version" not found')

        if spec['version'] != '0.1':
            raise InvalidSpecificationError('version must be "0.1"')

        if 'sources' not in spec:
            raise InvalidSpecificationError('At least one source is required')

        if 'scenarios' not in spec:
            raise InvalidSpecificationError('At least one scenario is required')
        #TODO: more

    def load_spec(self, raw_spec):
        self._validate_spec(raw_spec)
        self.spec['version'] = raw_spec['version']

        if 'identifiers' in raw_spec:
            self.spec['identifiers'] = self._load_spec_identifiers(raw_spec['identifiers'])

        if 'sources' in raw_spec:
            self.spec['sources'] = self._load_spec_sources(raw_spec['sources'])


    @staticmethod
    def _load_spec_identifiers(raw_spec_identifiers):
        spec_identifiers = {}
        for identifier_name, attributes in raw_spec_identifiers.items():
            spec_identifiers[identifier_name] = Identifier(attributes)
        return spec_identifiers

    @staticmethod
    def _load_spec_sources(raw_spec_sources):
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

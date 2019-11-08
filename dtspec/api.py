import networkx
import jsonschema
from colorama import Fore, Style

from dtspec.core import Identifier, Factory, Source, Target, Scenario, Case
from dtspec.expectations import DataExpectation

SCHEMA = {
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
                            "attribute": {"type": "string"},
                        },
                    },
                },
            },
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
                },
            },
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
                    "value": {"type": ["string", "null"]},
                },
            },
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
                            "values": {"$ref": "#/definitions/column_values"},
                            "by": {"type": "array", "items": {"type": "string"}},
                            "compare_via": {"type": "string"},
                        },
                    },
                }
            },
        },
    },
    "type": "object",
    "properties": {
        "version": {"type": "string"},
        "description": {"type": "string"},
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
                                "generator": {"type": "string"},
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
                    "identifier_map": {"$ref": "#/definitions/identifier_map"},
                    "description": {"type": "string"},
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
                    "identifier_map": {"$ref": "#/definitions/identifier_map"},
                    "description": {"type": "string"},
                },
            },
        },
        "factories": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "object",
                "required": ["factory"],
                "additionalProperties": False,
                "properties": {
                    "factory": {"type": "string"},
                    "description": {"type": "string"},
                    "parents": {"type": "array", "items": {"type": "string"}},
                    "data": {"$ref": "#/definitions/factory_data"},
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
                    "factory": {
                        "parents": {"type": "array", "items": {"type": "string"}},
                        "data": {"$ref": "#/definitions/factory_data"},
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
                                    },
                                },
                                "expected": {"$ref": "#/definitions/expected"},
                            },
                        },
                    },
                },
            },
        },
        "metadata": {"type": "object"},
    },
    "required": ["version", "sources", "scenarios"],
    "additionalProperties": False,
}


class ApiValidationError(Exception):
    pass


class ApiDuplicateError(ApiValidationError):
    pass


class ApiReferentialError(ApiValidationError):
    pass


class Api:
    def __init__(self, json_spec):
        self.json_spec = json_spec
        self.spec = {}
        self._parse_spec(self.json_spec)

    def _parse_spec(self, json_spec):
        "Converts the raw JSON spec into internal objects used to generate source data and run assertions"
        jsonschema.validate(json_spec, SCHEMA)

        self.spec["version"] = json_spec["version"]
        self.spec["description"] = json_spec.get("description", "")
        self._parse_spec_identifiers(json_spec)
        self._parse_spec_sources(json_spec)
        self._parse_spec_targets(json_spec)
        self._parse_spec_factories(json_spec)
        self._parse_spec_scenarios(json_spec)

    def generate_sources(self):
        "Used to generate all source data that will be passed back to the user"
        for _scenario_name, scenario in self.spec["scenarios"].items():
            scenario.generate()

    def source_data(self):
        return {
            name: source.serialize() for name, source in self.spec["sources"].items()
        }

    def load_actuals(self, actuals_json):
        """
        Used to load data containing the actual results to be compared with expectations

        Expecting a json object with keys that are the names of data targets and the
        values contain the individual records for the target, and description of the
        columns present in the target.

        Example::

            {
                "students": {
                    "records": [
                        {"id": "1", "name": "Buffy", "school_id": "1"},
                        {"id": "2", "name": "Willow", "school_id": "1"},
                    ],
                    "columns": ["id", "name", "school_id"]
                },
                "schools": {
                    "records": [
                        {"id": "1", "name": "Sunnydale"}
                    ],
                    "columns": ["id", "name"]
                }
            }
        """

        for target, target_data in actuals_json.items():
            records = target_data["records"]
            columns = target_data.get("columns", None)

            print(f"Loading actuals for target {target}")
            self.spec["targets"][target].load_actual(records, columns=columns)

    def assert_expectations(self):
        "Runs all of the assertions defined in the spec against the actual data"
        has_error = False

        for _, scenario in self.spec["scenarios"].items():
            for _, case in scenario.cases.items():
                print(f"Asserting {case.name}", end=" ")
                try:
                    case.assert_expectations()
                    print(Fore.GREEN + "PASSED" + Style.RESET_ALL)
                except AssertionError as err:
                    print(Fore.RED + "FAILED")
                    print(err)
                    print(Style.RESET_ALL)
                    has_error = True
        if has_error:
            raise AssertionError("There were dtspec assertion errors, please see log")

    def _parse_spec_identifiers(self, json_spec):
        self.spec["identifiers"] = {}

        for identifier_json in json_spec.get("identifiers", []):
            identifier_name = identifier_json["identifier"]
            attr_list = identifier_json["attributes"]

            if identifier_name in self.spec["identifiers"]:
                raise ApiDuplicateError(
                    f"Duplicate identifiers detected: {identifier_name}"
                )

            attributes = {}
            for attr in attr_list:
                attr_name = attr["field"]
                attr_args = {k: v for k, v in attr.items() if k != "field"}
                attributes[attr_name] = attr_args

            self.spec["identifiers"][identifier_name] = Identifier(
                attributes, name=identifier_name
            )

    def _parse_spec_sources(self, json_spec):
        self.spec["sources"] = {}
        for source_json in json_spec["sources"]:
            source_name = source_json["source"]

            defaults = {}
            for default in source_json.get("defaults", {}):
                defaults[default["column"]] = default["value"]

            id_mapping = self._parse_identifier_map(
                source_json.get("identifier_map", []), "source", source_name
            )

            self.spec["sources"][source_name] = Source(
                defaults=defaults,
                id_mapping=id_mapping,
                name=source_name,
                description=source_json.get("description", ""),
                identifiers=self.spec["identifiers"],
            )

    def _parse_identifier_map(self, map_json, data_type=None, data_name=None):
        id_mapping = {}
        for id_map in map_json:
            identifier_name = id_map["identifier"]["name"]
            identifier_attribute = id_map["identifier"]["attribute"]
            if identifier_name not in self.spec["identifiers"]:
                raise ApiReferentialError(
                    f'Unable to find identifier "{identifier_name}" referenced in {data_type}: "{data_name}"'
                )

            if (
                identifier_attribute
                not in self.spec["identifiers"][identifier_name].attributes
            ):
                raise ApiReferentialError(
                    f'Identifier attribute "{identifier_attribute}" referenced in {data_type}: "{data_name}" '
                    + f'not present for identifier "{identifier_name}"'
                )
            id_mapping[id_map["column"]] = {
                "identifier": self.spec["identifiers"][identifier_name],
                "attribute": identifier_attribute,
            }
        return id_mapping

    def _parse_spec_targets(self, json_spec):
        self.spec["targets"] = {}
        for target_json in json_spec["targets"]:
            target_name = target_json["target"]
            id_mapping = self._parse_identifier_map(
                target_json.get("identifier_map", []), "target", target_name
            )

            self.spec["targets"][target_name] = Target(
                id_mapping=id_mapping,
                name=target_name,
                description=target_json.get("description", ""),
            )

    @staticmethod
    def _sort_factories(factories):
        graph = networkx.DiGraph()
        for factory in factories:
            graph.add_node(factory["factory"])
            if "parents" not in factory:
                continue
            for parent in factory["parents"]:
                graph.add_edge(parent, factory["factory"])

        sorted_factory_names = [node for node in networkx.topological_sort(graph)]
        return sorted(
            factories,
            key=lambda factory: sorted_factory_names.index(factory["factory"]),
        )

    def _parse_spec_factories(self, json_spec):
        self.spec["factories"] = {}

        for factory_json in self._sort_factories(json_spec.get("factories", [])):
            factory_name = factory_json["factory"]
            factory_data = []
            if "data" in factory_json:
                factory_data = self._parse_spec_factory_data(
                    factory_json["data"], factory_name
                )

            if factory_name in self.spec["factories"].keys():
                raise ApiDuplicateError(f"Duplicate factories detected: {factory_name}")

            inherit_from = self._parse_spec_factory_parents(
                factory_json.get("parents", []), factory_name
            )

            self.spec["factories"][factory_name] = Factory(
                data=factory_data,
                inherit_from=inherit_from,
                sources=self.spec["sources"],
                name=factory_name,
                description=factory_json.get("description", ""),
            )

    def _parse_spec_factory_parents(self, parent_names, factory_name):
        inherit_from = []
        for parent_factory in parent_names:
            if parent_factory not in self.spec["factories"].keys():
                raise ApiReferentialError(
                    f'Unable to find parent factory "{parent_factory}" referenced in factory "{factory_name}"'
                )

            inherit_from.append(self.spec["factories"][parent_factory])
        return inherit_from

    def _parse_spec_factory_data(self, json_spec, factory_name):
        if not json_spec:
            return None

        spec = {}
        for data_json in json_spec:
            source_name = data_json["source"]
            if source_name not in self.spec["sources"].keys():
                raise ApiReferentialError(
                    f'Unable to find source "{source_name}" referenced in factory "{factory_name}"'
                )
            spec[source_name] = {
                "table": data_json.get("table", None),
                "values": {
                    value["column"]: value["value"]
                    for value in data_json.get("values", [])
                },
            }

        return spec

    def _parse_spec_scenarios(self, json_spec):
        self.spec["scenarios"] = {}
        for scenario_json in json_spec["scenarios"]:
            scenario_name = scenario_json["scenario"]

            if scenario_name in self.spec["scenarios"].keys():
                raise ApiDuplicateError(
                    f"Duplicate scenarios detected: {scenario_name}"
                )

            factory_json = scenario_json.get("factory")
            scenario_factory = None
            if factory_json:
                factory_name = f"Factory for Scenario {scenario_name}"
                scenario_factory = Factory(
                    inherit_from=self._parse_spec_factory_parents(
                        factory_json.get("parents", []), factory_name
                    ),
                    data=self._parse_spec_factory_data(
                        factory_json.get("data", []), factory_name
                    ),
                    sources=self.spec["sources"],
                )

            self.spec["scenarios"][scenario_name] = Scenario(
                name=scenario_name,
                cases=self._parse_spec_cases(
                    scenario_json["cases"], scenario_name, scenario_factory
                ),
                description=scenario_json.get("description", ""),
            )

    def _parse_spec_cases(self, cases_json, scenario_name, scenario_factory):
        cases = {}
        for case_json in cases_json:
            case_name = case_json["case"]
            if case_name in cases:
                raise ApiDuplicateError(
                    f'Duplicate cases detected in scenario "{scenario_name}": {case_name}'
                )

            case_data = self._parse_spec_factory_data(
                case_json.get("factory", {}).get("data", []),
                f"<Case Factory> {scenario_name}: {case_name}",
            )
            cases[case_name] = Case(
                name=f"{scenario_name}: {case_name}",
                factory=Factory(
                    sources=self.spec["sources"],
                    inherit_from=[scenario_factory] if scenario_factory else None,
                    data=case_data,
                ),
                expectations=self._parse_spec_expectations(case_json["expected"]),
                description=case_json.get("description", ""),
            )
        return cases

    def _parse_spec_expectations(self, expectations_json):
        expectations = []
        for expected_data in expectations_json["data"]:
            target_name = expected_data["target"]
            if target_name not in self.spec["targets"]:
                raise ApiReferentialError(
                    f'Unable to find target "{target_name}" referenced in expectation'
                )

            constants = {}
            if "values" in expected_data:
                constants = {
                    constant["column"]: constant["value"]
                    for constant in expected_data["values"]
                }

            target = self.spec["targets"][target_name]
            expectations.append(
                DataExpectation(
                    target=target,
                    table=expected_data["table"],
                    values=constants,
                    by=expected_data.get("by", []),
                    compare_via=expected_data.get("compare_via", None),
                    identifiers=self.spec["identifiers"],
                )
            )
        return expectations

    def to_markdown(self):  # pylint: disable=too-many-branches
        """
        Converts the user specs into a markdown representation that can be used for documentation
        """

        indent = lambda n, text: "\n".join(
            [" " * n + line for line in text.split("\n")]
        )

        doc = []
        doc.append("# Data Transform Spec\n")
        doc.append(self.json_spec["description"])

        doc.append("Data sources:\n")
        for source in self.json_spec["sources"]:
            doc.append(f"* {source['source']} - {source.get('description', '')}")
        doc.append("")

        doc.append("Data targets:\n")
        for target in self.json_spec["targets"]:
            doc.append(f"* {target['target']} - {target.get('description', '')}")
        doc.append("")

        doc.append("## Factories common to all scenarios\n")
        for factory in self.json_spec["factories"]:
            doc.append(f"### Factory: {factory['factory']}")
            doc.append(f"{factory.get('description', '')}")
            doc.append("")

            for source in factory.get("data", []):
                doc.append(f"**{source['source']}**:\n")
                doc.append(f"{source['table']}")

        for scenario in self.json_spec["scenarios"]:
            doc.append(f"# Scenario: {scenario['scenario']}\n")
            doc.append(f"Description: {scenario.get('description', '')}")

            doc.append("### Factory common to all cases in this scenario\n")
            if "parents" in scenario["factory"]:
                doc.append("Parents:\n")
                for parent in scenario["factory"]["parents"]:
                    doc.append(f"* {parent}")
                doc.append("")
            for source in scenario["factory"].get("data", []):
                doc.append(f"**{source['source']}**:\n")
                doc.append(f"{source['table']}")
            doc.append("")

            for case in scenario["cases"]:
                doc.append(f"## Case: {case['case']}\n")
                doc.append(f"Description: {case.get('description', '')}")

                if "factory" in case:
                    doc.append("* Given the source data\n")
                    for source in case["factory"].get("data", []):
                        doc.append(indent(2, f"**{source['source']}**:\n"))
                        doc.append(indent(2, f"{source['table']}"))

                doc.append("* Expected target data\n")
                for target in case["expected"].get("data", []):
                    doc.append(indent(2, f"**{target['target']}**:\n"))
                    doc.append(indent(2, f"{target['table']}"))
                    if "values" in target:
                        doc.append(indent(2, "Expected constant values:\n"))
                    for constant in target.get("values", []):
                        doc.append(
                            indent(
                                4, f"* **{constant['column']}**: {constant['value']}"
                            )
                        )

        return "\n".join(doc)

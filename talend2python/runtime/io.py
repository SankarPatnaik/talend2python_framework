"""Helpers for Talend connection types.

This module centralises the list of Talend connector names so that runtime
logic can easily check whether a particular connection represents normal data
flow or a special link such as ``COMPONENT_OK`` or ``RUN_IF``.  The mapping does
not currently implement behaviour for the connectors but provides a single
reference point which can be extended in the future.
"""

SUPPORTED_CONNECTORS = {
    "COMPONENT_ERROR",
    "COMPONENT_OK",
    "DUPLICATE",
    "FILTER",
    "FLOW",
    "ITERATE",
    "PARALLELIZE",
    "REJECT",
    "RUN_IF",
    "SUBJOB_ERROR",
    "SUBJOB_OK",
    "SYNCHRONIZE",
    "UNIQUE",
}

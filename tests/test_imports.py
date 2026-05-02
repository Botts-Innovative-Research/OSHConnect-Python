"""Public-API smoke tests: every name in `oshconnect.__all__` and
`oshconnect.csapi4py.__all__` is importable from its package, and the
re-exports we document users relying on actually resolve.
"""
import importlib

import pytest

# (package, names) — the documented public surface, grouped by concern.
EXPECTED_REEXPORTS = [
    ("oshconnect", ["OSHConnect", "Node", "System", "Datastream", "ControlStream"]),
    ("oshconnect", ["StreamableModes", "Status"]),
    ("oshconnect", ["TimePeriod", "TimeInstant", "TemporalModes", "TimeUtils"]),
    ("oshconnect", ["SystemResource", "DatastreamResource", "ControlStreamResource",
                    "ObservationResource"]),
    ("oshconnect", ["DataRecordSchema", "VectorSchema", "QuantitySchema",
                    "TimeSchema", "BooleanSchema", "CountSchema", "CategorySchema",
                    "TextSchema", "QuantityRangeSchema", "TimeRangeSchema"]),
    ("oshconnect", ["SWEDatastreamRecordSchema", "JSONCommandSchema"]),
    ("oshconnect", ["EventHandler", "IEventListener", "DefaultEventTypes",
                    "AtomicEventTypes", "Event", "EventBuilder"]),
    ("oshconnect", ["ObservationFormat", "APIResourceTypes", "ContentTypes"]),
    ("oshconnect.csapi4py", ["APIResourceTypes", "ObservationFormat", "ContentTypes",
                             "APITerms", "SystemTypes"]),
    ("oshconnect.csapi4py", ["ConnectedSystemsRequestBuilder",
                             "ConnectedSystemAPIRequest"]),
    ("oshconnect.csapi4py", ["MQTTCommClient"]),
    ("oshconnect.csapi4py", ["APIHelper"]),
]


@pytest.mark.parametrize(
    "package,names",
    EXPECTED_REEXPORTS,
    ids=[f"{pkg}:{','.join(names[:2])}{'…' if len(names) > 2 else ''}"
         for pkg, names in EXPECTED_REEXPORTS],
)
def test_documented_reexports_resolve(package, names):
    mod = importlib.import_module(package)
    for name in names:
        assert hasattr(mod, name), (
            f"{package} is expected to re-export {name!r} but does not"
        )
        assert getattr(mod, name) is not None


@pytest.mark.parametrize("package", ["oshconnect", "oshconnect.csapi4py"])
def test_all_list_present_and_complete(package):
    mod = importlib.import_module(package)
    assert hasattr(mod, "__all__"), f"{package} has no __all__"
    assert len(mod.__all__) > 0, f"{package}.__all__ is empty"
    for name in mod.__all__:
        assert hasattr(mod, name), (
            f"{package}.__all__ lists {name!r} but it is not importable"
        )
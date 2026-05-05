# Constructing wrappers

A `System`, `Datastream`, or `ControlStream` wrapper is a thin shell
around a pydantic resource model (`SystemResource`,
`DatastreamResource`, `ControlStreamResource`) plus a `Node` for HTTP /
MQTT / streaming context. Wrappers handle node-attached operations
(insertion, MQTT pub/sub, schema fetches over HTTP, storage-layer
round-trip); **format conversion lives entirely on the resource
models**.

That separation drives the construction story: build the resource via
the resource model's parsers, then bind it to a parent node via the
wrapper's constructor / factory.

## At-a-glance matrix

```{list-table}
:header-rows: 1
:widths: 26 26 26 22

* - Input
  - System
  - Datastream
  - ControlStream
* - Individual fields<br/>(new local resource)
  - `System(name=, label=, urn=, parent_node=, …)`
  - via `System.add_insert_datastream(DataRecordSchema)` — also POSTs server-side
  - via `System.add_and_insert_control_stream(DataRecordSchema, input_name=…)` — also POSTs server-side
* - Parsed `*Resource` model
  - `System.from_resource(sys_res, node)`<br/>(`from_system_resource` is deprecated)
  - `Datastream(parent_node=node, datastream_resource=ds_res)`<br/>(`from_resource` is deprecated)
  - `ControlStream(node=node, controlstream_resource=cs_res)`
* - Storage dict<br/>(round-trip from `to_storage_dict`)
  - `System.from_storage_dict(data, node)`
  - `Datastream.from_storage_dict(data, node)`
  - `ControlStream.from_storage_dict(data, node)`
```

For raw CS API JSON, parse it through the resource model first:

```{list-table}
:header-rows: 1
:widths: 30 70

* - Raw input
  - Resource-model parser
* - SML+JSON dict
  - `SystemResource.from_smljson_dict(data)`
* - GeoJSON dict
  - `SystemResource.from_geojson_dict(data)`
* - Any system shape (auto-detect)
  - `SystemResource.from_csapi_dict(data)`
* - CS API datastream dict
  - `DatastreamResource.from_csapi_dict(data)`
* - CS API control-stream dict
  - `ControlStreamResource.from_csapi_dict(data)`
* - Single OM+JSON observation
  - `ObservationResource.from_omjson_dict(data)`
* - Single SWE+JSON observation
  - `ObservationResource.from_swejson_dict(data, schema=…, result_time=…)`
* - SWE+JSON schema document
  - `SWEDatastreamRecordSchema.from_swejson_dict(data)`
* - OM+JSON schema document
  - `JSONDatastreamRecordSchema.from_omjson_dict(data)`
* - OSH logical schema (`obsFormat=logical`)
  - `LogicalDatastreamRecordSchema.from_logical_dict(data)`
```

## When to use which

### "I'm building a brand-new system from scratch"

Use the `System` constructor directly, then `insert_self()` (or let
`OSHConnect.create_and_insert_system(...)` do both). The wrapper
generates a `SystemResource` internally via `to_system_resource()`.

```python
from oshconnect import Node, System

node = Node(protocol='http', address='localhost', port=8282,
            username='admin', password='admin')
sys = System(
    name='WeatherStation',
    label='Weather Station #1',
    urn='urn:osh:sensor:weather:001',
    parent_node=node,
)
sys.insert_self()                  # POST /systems
print(sys.get_streamable_id())     # local UUID
print(sys._resource_id)            # server-assigned ID from Location header
```

### "I just got a JSON response back from a CS API server"

Two steps: parse the JSON via the matching resource-model factory, then
hand the resource to the wrapper.

```python
import requests
from oshconnect import Node, System, Datastream
from oshconnect.resource_datamodels import SystemResource, DatastreamResource

node = Node(protocol='http', address='localhost', port=8282,
            username='admin', password='admin')

# System: SML+JSON or GeoJSON, auto-detected by the resource model
resp = requests.get('http://localhost:8282/sensorhub/api/systems/abc')
sys = System.from_resource(SystemResource.from_csapi_dict(resp.json()), node)

# Datastream: single shape (application/json)
resp = requests.get('http://localhost:8282/sensorhub/api/datastreams/def')
ds = Datastream(
    parent_node=node,
    datastream_resource=DatastreamResource.from_csapi_dict(resp.json()),
)
```

If you already know the format and want to skip the auto-detect, swap
in `from_smljson_dict(...)` / `from_geojson_dict(...)` on
`SystemResource`. The wrapper layer doesn't care — it just receives a
pydantic model.

### "I have a `*Resource` already in memory"

```python
from oshconnect import Datastream, ControlStream, System

# System — `from_resource` binds a parsed SystemResource to a node
sys = System.from_resource(sys_resource, node)

# Datastream — constructor takes the parsed resource directly
ds = Datastream(parent_node=node, datastream_resource=ds_resource)

# ControlStream — same pattern
cs = ControlStream(node=node, controlstream_resource=cs_resource)
```

`System.from_resource` handles both wire shapes that round-trip through
`SystemResource` — the GeoJSON form (with name/uid under `properties`)
and the SML form (label/uid directly on the resource). The deprecated
`System.from_system_resource` emits a `DeprecationWarning` and is a
shim for `from_resource`.

### "I want to dump the wrapper back to JSON"

Reach down to the resource model. Format conversion isn't on the
wrapper:

```python
sys.to_system_resource().to_smljson_dict()       # SML+JSON
sys.to_system_resource().to_geojson_dict()       # GeoJSON
ds._underlying_resource.to_csapi_dict()           # datastream resource body
cs._underlying_resource.to_csapi_dict()           # control-stream resource body

# Schema documents: through the schema model
ds._underlying_resource.record_schema.to_swejson_dict()
ds._underlying_resource.record_schema.to_omjson_dict()
cs._underlying_resource.command_schema.to_json_dict()
```

### "I want the schema for an existing datastream from the server"

`Datastream` has three dedicated fetch methods, one per `obsFormat`
the server supports. Each returns a typed schema model so there's no
runtime auto-dispatch:

```python
ds = Datastream(parent_node=node, datastream_resource=DatastreamResource.from_csapi_dict(server_response))

# Wire-format schemas (CS API spec)
sw = ds.fetch_swejson_schema()    # -> SWEDatastreamRecordSchema (application/swe+json)
om = ds.fetch_omjson_schema()     # -> JSONDatastreamRecordSchema (application/om+json)

# OSH-specific JSON Schema flavor
lg = ds.fetch_logical_schema()    # -> LogicalDatastreamRecordSchema (obsFormat=logical)
```

Each method:

1. Hits ``GET /datastreams/{id}/schema?obsFormat={format}`` using the
   parent `Node`'s `APIHelper` for base URL + auth.
2. Parses the response into the corresponding pydantic model.
3. Returns the parsed model — does *not* mutate the datastream's
   `_underlying_resource.record_schema`. If you want to cache it, do
   it explicitly.

The **logical schema** is OSH-specific (not in the OGC CS API spec):
a JSON Schema document with OGC extension keywords
(`x-ogc-definition`, `x-ogc-refFrame`, `x-ogc-unit`, `x-ogc-axis`)
carrying the SWE Common metadata.

### "I'm restoring state from local storage"

`from_storage_dict()` rebuilds wrappers from the dicts produced by
`to_storage_dict()`. Used by `OSHConnect.load_config()` and the SQLite
datastore (`oshconnect.datastores.sqlite_store`); not what you want for
parsing CS API server responses (those have a different shape — use
the resource models for those).

```python
import json
from oshconnect import Node, System

with open('my_app_config.json') as f:
    cfg = json.load(f)

node = Node.from_storage_dict(cfg['nodes'][0])
for sys_dict in cfg['systems']:
    sys = System.from_storage_dict(sys_dict, node)
    node.add_new_system(sys)
```

## What about new datastreams/controlstreams without going through System?

The `Datastream(...)` and `ControlStream(...)` constructors require an
already-built resource object — there's no "build from individual fields"
path because building one of these correctly requires defining the
schema (`SWEDatastreamRecordSchema` or `JSONCommandSchema`) and threading
it through a `DatastreamResource` / `ControlStreamResource`. The
high-level entry points handle that for you:

- `System.add_insert_datastream(DataRecordSchema)` — wraps a schema as
  `SWEDatastreamRecordSchema` (with `JSONEncoding`), builds the
  `DatastreamResource`, POSTs to the server, and returns the `Datastream`.
- `System.add_and_insert_control_stream(DataRecordSchema, input_name=…)` —
  symmetric for ControlStreams via `JSONCommandSchema`.

If you really want to build from scratch without inserting, copy what
those two methods do (see `streamableresource.py` for the recipe).

## Why no `to_*_dict` / `from_*_dict` on the wrappers?

Because format conversion is the resource model's job. Keeping it there
gives one canonical entry point per format, so there's no question of
"is `System.to_smljson_dict()` the same as `system_resource.to_smljson_dict()`?"
— there's only the latter. The wrapper's job is to bind a resource to a
parent node and run the operations that need that node (HTTP, MQTT,
storage). Two layers, two responsibilities.

The deprecated `System.from_system_resource` and `Datastream.from_resource`
shims remain for one release as compatibility — both delegate to the new
canonical paths.

## See also

- [Class hierarchy](class_hierarchy.md) — the type relationships among
  wrappers, resource models, and schema documents.
- [Insertion sequence](insertion.md) — the POST flow that follows
  construction when you want to push a new resource server-side.
- [Serialization](serialization.md) — the format-explicit `to_*_dict`
  / `from_*_dict` methods on the resource models, including the OGC
  format coverage matrix.

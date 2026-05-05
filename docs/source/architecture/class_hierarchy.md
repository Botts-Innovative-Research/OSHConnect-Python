# Class hierarchy

OSHConnect's type system has three roughly-orthogonal trees: the
**user-facing wrappers** (`Node`, `System`, `Datastream`, `ControlStream`),
the **CS API resource models** that those wrappers serialize to/from on the
wire, and the **SWE Common schema components** that describe the shape of
observations and commands.

## Wrapper hierarchy

The wrapper classes are in `streamableresource.py`. `StreamableResource[T]`
is an abstract, generic base — `T` is the underlying pydantic resource
model the wrapper holds (`SystemResource`, `DatastreamResource`, or
`ControlStreamResource`). The base manages the MQTT subscribe/publish
plumbing and inbound/outbound deques common to all three concretions.

```mermaid
classDiagram
    direction TB
    class Node {
        +protocol: str
        +address: str
        +port: int
        +discover_systems()
        +add_system()
        +get_api_helper() APIHelper
        +to_storage_dict() dict
    }
    class StreamableResource~T~ {
        <<abstract>>
        +get_streamable_id() UUID
        +initialize()
        +start()
        +stop()
        +subscribe_mqtt(topic)
        +publish(payload, topic)
        +to_storage_dict() dict
    }
    class System {
        +name: str
        +urn: str
        +datastreams: list~Datastream~
        +control_channels: list~ControlStream~
        +discover_datastreams()
        +add_insert_datastream()
        +to_smljson_dict() dict
        +to_geojson_dict() dict
    }
    class Datastream {
        +get_id() str
        +create_observation()
        +observation_to_omjson_dict()
        +observation_to_swejson_dict()
    }
    class ControlStream {
        +publish_command()
        +publish_status()
        +command_to_json_dict()
        +command_to_swejson_dict()
    }

    Node "1" o-- "*" System : owns
    System "1" o-- "*" Datastream : owns
    System "1" o-- "*" ControlStream : owns

    StreamableResource <|-- System
    StreamableResource <|-- Datastream
    StreamableResource <|-- ControlStream
```

`Node` is intentionally *not* a `StreamableResource` — it's a connection
holder, not a streamable.

## CS API resource models

Pydantic models in `resource_datamodels.py`. Each is what `model_dump(by_alias=True)`
produces a CS API JSON body from, and what `model_validate(data, by_alias=True)`
parses a server response into. The wrapper classes above hold one of these
as `_underlying_resource`.

```mermaid
classDiagram
    direction TB
    class BaseModel {
        <<pydantic>>
        +model_dump()
        +model_validate()
    }
    class BaseResource {
        +id: str
        +name: str
        +description: str
        +type: str
        +links: List~Link~
    }
    class SystemResource {
        +feature_type: str  // "PhysicalSystem" or "Feature"
        +system_id: str
        +uid: str
        +label: str
        +to_smljson_dict()
        +to_geojson_dict()
        +from_csapi_dict() classmethod
    }
    class DatastreamResource {
        +ds_id: str
        +name: str
        +valid_time: TimePeriod
        +record_schema: DatastreamRecordSchema
        +to_csapi_dict()
        +from_csapi_dict() classmethod
    }
    class ControlStreamResource {
        +cs_id: str
        +input_name: str
        +command_schema: CommandSchema
        +to_csapi_dict()
        +from_csapi_dict() classmethod
    }
    class ObservationResource {
        +result_time: TimeInstant
        +phenomenon_time: TimeInstant
        +result: dict
        +to_omjson_dict()
        +to_swejson_dict()
    }

    BaseModel <|-- BaseResource
    BaseModel <|-- SystemResource
    BaseModel <|-- DatastreamResource
    BaseModel <|-- ControlStreamResource
    BaseModel <|-- ObservationResource
```

The `record_schema` / `command_schema` slots are typed
`SerializeAsAny[DatastreamRecordSchema]` /
`SerializeAsAny[CommandSchema]` so they preserve discriminated-union
polymorphism on dump — see the schema document tree below.

## Schema documents

`schema_datamodels.py` defines the polymorphic schema wrappers that live
inside `DatastreamResource.record_schema` and
`ControlStreamResource.command_schema`. The discriminator is the format
field (`obs_format` or `command_format`).

```mermaid
classDiagram
    direction TB
    class DatastreamRecordSchema {
        <<abstract>>
        +obs_format: str
    }
    class SWEDatastreamRecordSchema {
        +obs_format = "application/swe+json"
        +encoding: Encoding
        +record_schema: AnyComponent
    }
    class JSONDatastreamRecordSchema {
        +obs_format = "application/om+json"
        +result_schema: AnyComponent
        +parameters_schema: AnyComponent
    }

    class CommandSchema {
        <<abstract>>
        +command_format: str
    }
    class SWEJSONCommandSchema {
        +command_format = "application/swe+json"
        +encoding: Encoding
        +record_schema: AnyComponent
    }
    class JSONCommandSchema {
        +command_format = "application/json"
        +params_schema: AnyComponent
        +result_schema: AnyComponent
        +feasibility_schema: AnyComponent
    }

    DatastreamRecordSchema <|-- SWEDatastreamRecordSchema
    DatastreamRecordSchema <|-- JSONDatastreamRecordSchema
    CommandSchema <|-- SWEJSONCommandSchema
    CommandSchema <|-- JSONCommandSchema
```

Each variant has a `to_*_dict()` / `from_*_dict()` convenience method
matching its format — see [Serialization](serialization.md).

## SWE Common component union

`swe_components.py` defines the SWE Common Data Model component types as a
discriminated union (`AnyComponent = Annotated[Union[...], Field(discriminator="type")]`).
The `type` literal on each subclass routes pydantic to the right concrete
class on parse.

```mermaid
classDiagram
    direction TB
    class AnyComponentSchema {
        +type: str
        +id: str
        +name: str
        +label: str
        +description: str
    }
    class AnySimpleComponentSchema {
        +reference_frame: str
        +axis_id: str
        +nil_values: list
    }
    class AnyScalarComponentSchema
    class DataRecordSchema {
        +type = "DataRecord"
        +fields: list~AnyComponent~
    }
    class VectorSchema {
        +type = "Vector"
        +reference_frame: str
        +coordinates: list~Count|Quantity|Time~
    }
    class DataArraySchema {
        +type = "DataArray"
        +element_type: AnyComponent
    }
    class DataChoiceSchema {
        +type = "DataChoice"
        +items: list~AnyComponent~
    }
    class GeometrySchema {
        +type = "Geometry"
        +srs: str
    }
    class QuantitySchema {
        +type = "Quantity"
        +uom: UCUMCode|URI
    }
    class BooleanSchema {
        +type = "Boolean"
    }
    class CountSchema {
        +type = "Count"
    }
    class TimeSchema {
        +type = "Time"
        +uom: UCUMCode|URI
    }
    class TextSchema {
        +type = "Text"
    }
    class CategorySchema {
        +type = "Category"
    }

    AnyComponentSchema <|-- DataRecordSchema
    AnyComponentSchema <|-- VectorSchema
    AnyComponentSchema <|-- DataArraySchema
    AnyComponentSchema <|-- DataChoiceSchema
    AnyComponentSchema <|-- GeometrySchema
    AnyComponentSchema <|-- AnySimpleComponentSchema
    AnySimpleComponentSchema <|-- AnyScalarComponentSchema
    AnyScalarComponentSchema <|-- BooleanSchema
    AnyScalarComponentSchema <|-- CountSchema
    AnyScalarComponentSchema <|-- QuantitySchema
    AnyScalarComponentSchema <|-- TimeSchema
    AnyScalarComponentSchema <|-- CategorySchema
    AnyScalarComponentSchema <|-- TextSchema
```

(Range variants — `CountRangeSchema`, `QuantityRangeSchema`, `TimeRangeSchema`,
`CategoryRangeSchema` — extend `AnySimpleComponentSchema` directly and are
omitted from the diagram for brevity.)

## SoftNamedProperty

The `name` field is *not* a property of any data component itself per SWE
Common 3 — it lives on the `SoftNamedProperty` wrapper that binds a child
into a parent. OSHConnect enforces this via `@model_validator(mode="after")`
on the seven binding contexts: `DataRecord.fields`, `DataChoice.items`,
`Vector.coordinates`, `DataArray.elementType`, `Matrix.elementType`, and
the root recordSchema/resultSchema/parametersSchema of datastream and
control-stream wrappers.

See `tests/test_swe_components.py` for the full validation surface.

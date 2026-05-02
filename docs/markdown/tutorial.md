# Tutorial

OSHConnect-Python is a library for interacting with OpenSensorHub through
OGC API – Connected Systems. This tutorial walks through the most common
workflows.

## Installation

Install with `uv` (recommended):

```bash
uv add git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git
```

Or with `pip`:

```bash
pip install git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git
```

All public classes and utilities can be imported directly from `oshconnect`:

```python
from oshconnect import OSHConnect, Node, System, Datastream, ControlStream
from oshconnect import TimePeriod, TimeInstant, TemporalModes
from oshconnect import DataRecordSchema, QuantitySchema, TimeSchema, TextSchema
from oshconnect import ObservationFormat, DefaultEventTypes
```

## Creating an OSHConnect instance

The main entry point is the `OSHConnect` class:

```python
from oshconnect import OSHConnect, TemporalModes

app = OSHConnect(name='MyApp')
```

## Adding a Node

A `Node` represents a connection to a single OSH server. The `OSHConnect`
instance can manage multiple nodes simultaneously.

```python
from oshconnect import OSHConnect, Node

app = OSHConnect(name='MyApp')
node = Node(protocol='http', address='localhost', port=8585,
            username='test', password='test')
app.add_node(node)
```

To connect a node with MQTT support for streaming:

```python
node = Node(protocol='http', address='localhost', port=8585,
            username='test', password='test',
            enable_mqtt=True, mqtt_port=1883)
app.add_node(node)
```

## Discovery

Discover all systems available on all registered nodes:

```python
app.discover_systems()
```

Discover all datastreams across all discovered systems:

```python
app.discover_datastreams()
```

## Streaming observations (MQTT)

Once a node is configured with MQTT and datastreams are discovered, start
receiving observations by initializing and starting each datastream:

```python
from oshconnect import StreamableModes

for ds in app.get_datastreams():
    ds.set_connection_mode(StreamableModes.PULL)
    ds.initialize()
    ds.start()
```

Incoming messages are appended to each datastream's inbound deque:

```python
import time

time.sleep(2)  # allow messages to arrive
for ds in app.get_datastreams():
    while ds.get_inbound_deque():
        msg = ds.get_inbound_deque().popleft()
        print(msg)
```

## Resource event subscriptions

Subscribe to resource lifecycle events (create / update / delete) using
`subscribe_events()`. These arrive as CloudEvents v1.0 JSON payloads:

```python
def on_event(client, userdata, msg):
    print(f"Event on {msg.topic}: {msg.payload}")

for ds in app.get_datastreams():
    topic = ds.subscribe_events(callback=on_event)
    print(f"Subscribed to event topic: {topic}")
```

## Inserting a new System

```python
from oshconnect import OSHConnect, Node

app = OSHConnect(name='MyApp')
node = Node(protocol='http', address='localhost', port=8585,
            username='admin', password='admin')
app.add_node(node)

new_system = app.create_and_insert_system(
    system_opts={
        'name': 'Test System',
        'description': 'A test system',
        'uid': 'urn:system:test:001',
    },
    target_node=node
)
```

## Inserting a new Datastream

Build a schema using SWE Common component classes, then attach it to a system:

```python
from oshconnect import DataRecordSchema, TimeSchema, QuantitySchema, TextSchema
from oshconnect.api_utils import URI, UCUMCode

datarecord = DataRecordSchema(
    label='Example Record',
    description='Example datastream record',
    definition='http://example.org/records/example',
    fields=[]
)

# TimeSchema must be the first field for OSH
datarecord.fields.append(
    TimeSchema(label='Timestamp',
               definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
               name='timestamp',
               uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'))
)
datarecord.fields.append(
    QuantitySchema(name='distance', label='Distance',
                   definition='http://example.org/Distance',
                   uom=UCUMCode(code='m', label='meters'))
)
datarecord.fields.append(
    TextSchema(name='label', label='Label',
               definition='http://example.org/Label')
)

datastream = new_system.add_insert_datastream(datarecord)
```

!!! note
    A `TimeSchema` must be the first field in the `DataRecordSchema` when
    targeting OpenSensorHub.

## Inserting an Observation

Once a datastream is registered, send observation data using
`insert_observation_dict()`:

```python
from oshconnect import TimeInstant

datastream.insert_observation_dict({
    'resultTime': TimeInstant.now_as_time_instant().get_iso_time(),
    'phenomenonTime': TimeInstant.now_as_time_instant().get_iso_time(),
    'result': {
        'timestamp': TimeInstant.now_as_time_instant().epoch_time,
        'distance': 1.0,
        'label': 'example observation',
    }
})
```

!!! note
    The keys in `result` correspond to the `name` fields of each schema
    component. `resultTime` and `phenomenonTime` are required by
    OpenSensorHub.

## Saving and loading configuration

The OSHConnect state (nodes, systems, datastreams) can be persisted to a JSON
file:

```python
app.save_config()                   # saves to a default file
app = OSHConnect.load_config('my_config.json')
```
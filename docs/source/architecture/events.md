# Event system

OSHConnect has two pub/sub layers and they're easy to confuse:

- **MQTT pub/sub** — across the network. Datastreams subscribe to
  `:data` topics on the OSH server's MQTT broker; ControlStreams publish
  commands. Implemented via `paho-mqtt` in `csapi4py/mqtt.py`.
- **In-process EventHandler** — within the Python process. A singleton
  pub/sub bus that fans out `Event` objects to in-app listeners (e.g. a
  visualization widget that wants to know whenever a new observation
  arrives). Implemented in `events/`.

This page is about the second one. The two are connected: when a Datastream
receives an MQTT message, its `_emit_inbound_event(msg)` hook builds an
`Event` and publishes it to the in-process bus.

## Class diagram

```mermaid
classDiagram
    direction TB
    class EventHandler {
        <<singleton>>
        +listeners: list~IEventListener~
        +event_queue: deque~Event~
        +register_listener(listener)
        +unregister_listener(listener)
        +subscribe(callback, types, topics)
        +publish(event)
    }
    class IEventListener {
        <<abstract>>
        +topics: list~str~
        +types: list~DefaultEventTypes~
        +handle_events(event)*
    }
    class CallbackListener {
        +callback: Callable
        +handle_events(event)
    }
    class Event {
        +timestamp: datetime
        +type: DefaultEventTypes
        +topic: str
        +data: Any
        +producer: Any
    }
    class EventBuilder {
        -_event: Event
        +with_type(t)
        +with_topic(s)
        +with_data(d)
        +with_producer(p)
        +build() Event
    }
    class DefaultEventTypes {
        <<enum>>
        NEW_OBSERVATION
        NEW_COMMAND
        NEW_COMMAND_STATUS
        ADD_NODE / REMOVE_NODE
        ADD_SYSTEM / REMOVE_SYSTEM
        ADD_DATASTREAM / REMOVE_DATASTREAM
        ADD_CONTROLSTREAM / REMOVE_CONTROLSTREAM
    }

    EventHandler "1" o-- "*" IEventListener : holds
    IEventListener <|-- CallbackListener
    EventBuilder ..> Event : builds
    EventHandler ..> Event : dispatches
    Event --> DefaultEventTypes : typed by
```

`AtomicEventTypes` (CRUD verbs: CREATE, POST, GET, MODIFY, UPDATE, REMOVE,
DELETE) is a separate enum used for finer-grained sub-classification of
resource operations; it's not directly attached to `Event` but is available
for callers building their own event taxonomies.

## Subscribe → publish → dispatch

The handler is reentrancy-safe: if a listener calls `publish()` while the
handler is already inside another `publish()` (the `publish_lock` is held),
the new event is queued and drained after the current dispatch finishes.
Same for `register_listener` / `unregister_listener` mid-dispatch — they're
deferred to `to_add` / `to_remove` lists and flushed by `commit_changes()`.

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant H as EventHandler
    participant L as CallbackListener
    participant DS as Datastream
    participant MQTT as MQTT Broker

    Note over User,L: 1. Subscribe
    User->>H: subscribe(my_callback, types=[NEW_OBSERVATION])
    H->>L: CallbackListener(callback=my_callback, types=[NEW_OBSERVATION])
    H->>H: register_listener(L)

    Note over MQTT,L: 2. MQTT message arrives → in-process event
    MQTT-->>DS: paho-mqtt callback (msg)
    DS->>DS: _mqtt_sub_callback(msg)
    DS->>DS: _inbound_deque.append(msg.payload)
    DS->>DS: _emit_inbound_event(msg)
    DS->>DS: EventBuilder().with_type(NEW_OBSERVATION).with_topic(msg.topic)<br/>.with_data(msg.payload).with_producer(self).build()
    DS->>H: publish(evt)
    H->>H: publish_lock = True
    loop for each listener
        H->>H: _matches(listener, evt)?
        alt type & topic match
            H->>L: handle_events(evt)
            L->>User: my_callback(evt)
        end
    end
    H->>H: publish_lock = False<br/>commit_changes()  // drain queued events / listeners
```

## Subscribing in user code

Two styles, both call into the same `EventHandler` singleton:

**Functional (no subclassing):**

```python
from oshconnect import EventHandler, DefaultEventTypes

handler = EventHandler()

def on_observation(event):
    print(f"{event.topic}: {event.data!r}")

listener = handler.subscribe(
    on_observation,
    types=[DefaultEventTypes.NEW_OBSERVATION],
)
# later, to stop receiving:
handler.unregister_listener(listener)
```

**Subclass:**

```python
from oshconnect import EventHandler, IEventListener, DefaultEventTypes

class MyListener(IEventListener):
    def handle_events(self, event):
        ...

EventHandler().register_listener(
    MyListener(types=[DefaultEventTypes.ADD_SYSTEM])
)
```

Empty `types` or `topics` lists mean "match all" — the handler filters
before dispatching, so you don't need to filter inside your callback.

## What emits which events

| Source | Event type | Emitted from |
|---|---|---|
| Inbound observation on a Datastream's MQTT data topic | `NEW_OBSERVATION` | `Datastream._emit_inbound_event` |
| Inbound command on a ControlStream's command topic | `NEW_COMMAND` | `ControlStream._emit_inbound_event` |
| Inbound status on a ControlStream's status topic | `NEW_COMMAND_STATUS` | `ControlStream._emit_inbound_event` |
| Resource lifecycle events (`ADD_NODE`, `ADD_SYSTEM`, etc.) | matching `DefaultEventTypes` | currently emitted by the wrapper classes during construction / discovery (see `eventbus.py` re-exports for the full list) |

## See also

- `eventbus.py` re-exports `EventHandler`, `Event`, `EventBuilder`,
  `IEventListener`, `CallbackListener`, `DefaultEventTypes`, and
  `AtomicEventTypes` for convenient import from `oshconnect`.
- [Class hierarchy](class_hierarchy.md) for how the listener interface
  fits into the broader type system.

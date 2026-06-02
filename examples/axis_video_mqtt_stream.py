#!/usr/bin/env python
#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/21
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Live MQTT video viewer with selectable datastream / control stream.

Sibling to ``axis_video_frame.py``: that file pulls a fixed batch of
``application/swe+binary`` observations over HTTP and shows the codec is
byte-identical on round-trip. This one drives a camera datastream through
the full library end-to-end — `OSHConnect` discovery, `Node` with
``enable_mqtt=True``, a live MQTT subscription to the new
``…/observations:data/swe-binary`` topic — and decodes the incoming NAL
units live so the operator can actually see the camera moving.

Unlike the earlier revision (which showed a fixed multi-camera grid driven
entirely by ``OSHC_AXIS_CAMERAS``), the viewer now shows **one** video
panel plus two dropdowns:

* a **video datastream** dropdown listing every swe+binary video source
  discovered on the node, and
* a **control stream** dropdown listing every control stream discovered on
  the node (the PTZ buttons assume a PTZ rig — see the note on the panel).

Picking a different entry re-subscribes live: the viewer unsubscribes the
old MQTT topic and subscribes the newly chosen one without restarting. The
two selections round-trip through a small JSON config file
(``axis_video_config.json`` beside this script, overridable via
``OSHC_AXIS_CONFIG``): the dropdowns are pre-selected from it on launch and
written back whenever they change.

What it exercises
-----------------

* The new CS API Part 3 ``:data/<token>`` format subtopic — the video
  datastream subscribes to its swe-binary subtopic, not bare ``:data``.
* `Datastream.decode_observation` on each MQTT message payload — same codec
  the HTTP example uses, fed one record at a time from the broker.
* PyAV incremental decode of standalone H.264 NAL units (no container,
  no Annex B parsing on our side — PyAV's parser handles framing).
* Live re-subscription when the operator switches streams from the GUI.

Defaults
--------
* Node:        ``http://localhost:8282/sensorhub/api`` (HTTP)
*               ``localhost:1883`` (MQTT broker on the same host)

The initial selection resolves in this order: saved config → environment
defaults (``OSHC_AXIS_CAMERAS`` / ``OSHC_PTZ_CS_ID``) → first discovered
entry. On startup the resolved pair is written back, so a hand-edited
config that points at a stream no longer present on the node is silently
rewritten to the fallback (valid ids are left untouched).

Override with:

* ``OSHC_AXIS_HOST``       — server hostname/IP (default ``localhost``).
* ``OSHC_AXIS_PORT``       — HTTP API port (default ``8282``).
* ``OSHC_AXIS_MQTT_PORT``  — MQTT broker port (default ``1883``).
* ``OSHC_AXIS_USER`` / ``OSHC_AXIS_PASS`` — Basic-Auth credentials, if any.
* ``OSHC_AXIS_CAMERAS``    — comma-separated ``Label:datastream_id`` pairs;
  only the first entry's id is used as the initial video default.
* ``OSHC_PTZ_CS_ID``       — control-stream id to pre-select.
* ``OSHC_AXIS_CONFIG``     — path to the selection config JSON.
* ``OSHC_AXIS_RUN_SECS``   — auto-exit after this many seconds (default
  ``0`` = run until the window is closed).

Run
---
    uv run python examples/axis_video_mqtt_stream.py

Needs the ``[av]`` extra for H.264 decoding and tkinter for display::

    uv pip install -e ".[av]"
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from oshconnect import OSHConnect
from oshconnect.node import Node
from oshconnect.resources.base import StreamableModes
from oshconnect.resources.controlstream import ControlStream
from oshconnect.resources.datastream import Datastream
from oshconnect.schema_datamodels import (
    SWEBinaryDatastreamRecordSchema,
    SWEJSONCommandSchema,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HOST = os.environ.get("OSHC_AXIS_HOST", "localhost")
HTTP_PORT = int(os.environ.get("OSHC_AXIS_PORT", "8282"))
MQTT_PORT = int(os.environ.get("OSHC_AXIS_MQTT_PORT", "1883"))
USER = os.environ.get("OSHC_AXIS_USER") or None
PASS = os.environ.get("OSHC_AXIS_PASS") or None
RUN_SECS = float(os.environ.get("OSHC_AXIS_RUN_SECS", "0"))

# Where the dropdown selections round-trip to. Beside this script by
# default so it doesn't depend on the working directory; override with
# OSHC_AXIS_CONFIG. This is runtime state, not a committed artifact.
CONFIG_PATH = Path(
    os.environ.get("OSHC_AXIS_CONFIG", "")
    or str(Path(__file__).with_name("axis_video_config.json")))

# Control-stream ID for the PTZ rig. Default ``""`` means auto-discover by
# inputName ("ptzControl"). Set OSHC_PTZ_CS_ID to pin a specific stream when
# multiple cameras live on the same node.
PTZ_CS_ID = os.environ.get("OSHC_PTZ_CS_ID", "").strip() or None
# Step sizes for the relative-motion buttons — small enough that auto-mode
# pans within the safe envelope without thrashing the gimbal.
PTZ_PAN_STEP = float(os.environ.get("OSHC_PTZ_PAN_STEP", "5"))
PTZ_TILT_STEP = float(os.environ.get("OSHC_PTZ_TILT_STEP", "2"))
PTZ_ZOOM_STEP = float(os.environ.get("OSHC_PTZ_ZOOM_STEP", "1"))
# When set, the GUI fires a scripted sequence of PTZ commands and exits
# (`rpan -PTZ_PAN_STEP`, `rpan +2·STEP`, `rpan -PTZ_PAN_STEP`, …) so the
# round-trip can be verified in CI / headless terminals.
PTZ_AUTO = os.environ.get("OSHC_PTZ_AUTO", "").lower() in ("1", "true", "yes")


def _parse_camera_env(raw: str) -> list[tuple[str, str]]:
    """Parse ``Label1:id1,Label2:id2`` into a list of (label, ds_id) tuples."""
    out: list[tuple[str, str]] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" not in chunk:
            raise ValueError(
                f"OSHC_AXIS_CAMERAS entry {chunk!r} must be 'Label:datastream_id'.")
        label, ds = chunk.split(":", 1)
        out.append((label.strip(), ds.strip()))
    return out


# Only the first entry's datastream id is consulted, as the initial video
# default when no config file exists. Empty string → no env default.
_ENV_CAMERAS = _parse_camera_env(os.environ.get("OSHC_AXIS_CAMERAS", ""))
ENV_VIDEO_DS_ID = _ENV_CAMERAS[0][1] if _ENV_CAMERAS else None


# ---------------------------------------------------------------------------
# Selection config round-trip
# ---------------------------------------------------------------------------


def load_selection() -> dict:
    """Read the saved ``{video_datastream_id, control_stream_id}`` selection.

    Returns an empty dict when the file is missing or unreadable — the
    caller then falls back to environment defaults / first discovered entry.
    """
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        logging.warning("Could not read selection config %s: %s", CONFIG_PATH, exc)
        return {}


def save_selection(video_ds_id: Optional[str], control_cs_id: Optional[str]) -> None:
    """Persist the current dropdown selections so the next launch restores
    them. Best-effort: a write failure is logged, not raised — losing the
    persisted choice should never take the live viewer down."""
    payload = {
        "video_datastream_id": video_ds_id,
        "control_stream_id": control_cs_id,
    }
    try:
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logging.info("Saved selection to %s: %s", CONFIG_PATH, payload)
    except OSError as exc:
        logging.warning("Could not write selection config %s: %s", CONFIG_PATH, exc)


# ---------------------------------------------------------------------------
# Discovered-option containers
# ---------------------------------------------------------------------------


@dataclass
class VideoOption:
    """One selectable video datastream, kept resolved so the dropdown
    doesn't have to re-walk the system tree on every switch."""
    label: str
    ds_id: str
    datastream: Datastream


@dataclass
class ControlOption:
    """One selectable control stream."""
    label: str
    cs_id: str
    controlstream: ControlStream


@dataclass
class Holder:
    """Single-slot mutable reference. Lets GUI callbacks and the render
    loop read the *current* active object after a live swap without
    re-binding closures — the in-flight paho callback on a replaced object
    simply writes to the now-detached instance, harmlessly."""
    current: Any = None


# ---------------------------------------------------------------------------
# Per-camera state
# ---------------------------------------------------------------------------


@dataclass
class CameraStream:
    """Mutable state for one camera's live MQTT subscription."""
    label: str
    ds_id: str
    datastream: Optional[Datastream] = None
    # PyAV CodecContext handle (typed as ``object`` to avoid an import-time
    # PyAV dep on this file when the user only wants to read the source).
    codec_ctx: Optional["object"] = None
    # Per-camera frame queue: producer is the PyAV decode step (running in
    # the paho network thread); consumer is the tkinter render step.
    # A deque with maxlen=1 means "drop intermediate frames if the GUI
    # falls behind" — preferred over backing up.
    latest_frame: deque = field(default_factory=lambda: deque(maxlen=1))
    nals_received: int = 0
    frames_decoded: int = 0
    last_error: Optional[str] = None


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _system_label(system) -> str:
    """Display name for a `System` — its ``label`` (the CS API/SML display
    string), falling back to the resource id. Avoids `System.name`, which
    is deprecated."""
    return (getattr(system, "label", None)
            or getattr(system, "_resource_id", None) or "system")


def _system_id(system) -> str:
    """Server-side id of a `System` (``_resource_id``)."""
    return getattr(system, "_resource_id", None) or "?"


def connect_and_discover() -> tuple[OSHConnect, Node]:
    """Build an `OSHConnect` with one `Node` (MQTT enabled), discover the
    full system / datastream / control-stream tree, and return both for
    downstream wiring."""
    osh = OSHConnect(name="axis-mqtt-viewer")
    node = Node(
        protocol="http",
        address=HOST,
        port=HTTP_PORT,
        username=USER,
        password=PASS,
        enable_mqtt=True,
        mqtt_port=MQTT_PORT,
    )
    osh.add_node(node)
    osh.discover_systems()
    # Datastream + control-stream discovery is per-system.
    for system in node._systems:
        try:
            system.discover_datastreams()
        except Exception as exc:  # noqa: BLE001
            logging.error("Datastream discovery failed for system %s: %s",
                          _system_id(system), exc)
        try:
            system.discover_controlstreams()
        except Exception as exc:  # noqa: BLE001
            logging.error("ControlStream discovery failed for system %s: %s",
                          _system_id(system), exc)
    return osh, node


def is_swe_binary_video(ds: Datastream) -> bool:
    """A datastream is treated as a binary video source if its record
    schema is `SWEBinaryDatastreamRecordSchema` and exposes an ``img``
    block member (the Axis driver convention)."""
    schema = getattr(ds.get_resource(), "record_schema", None)
    if not isinstance(schema, SWEBinaryDatastreamRecordSchema):
        return False
    members = getattr(getattr(schema, "record_encoding", None), "members", [])
    return any(
        getattr(m, "ref", "").endswith("/img") or getattr(m, "ref", "") == "img"
        for m in members
    )


def discover_video_options(node: Node) -> list[VideoOption]:
    """Walk every system on the node and return one `VideoOption` per
    swe+binary video datastream, labelled ``<system> · <datastream>``."""
    out: list[VideoOption] = []
    for system in node._systems:
        sys_name = _system_label(system)
        for ds in system.datastreams:
            if not is_swe_binary_video(ds):
                continue
            ds_name = getattr(ds.get_resource(), "name", "") or ds.get_id()
            out.append(VideoOption(label=f"{sys_name} · {ds_name}",
                                   ds_id=ds.get_id(), datastream=ds))
    return out


def discover_control_options(node: Node) -> list[ControlOption]:
    """Return one `ControlOption` per discovered control stream, labelled
    ``<system> · <inputName>``. PTZ-style streams (``inputName ==
    'ptzControl'``) sort first so the default selection lands on one."""
    out: list[ControlOption] = []
    for system in node._systems:
        sys_name = _system_label(system)
        for cs in system.control_channels:
            res = cs.get_underlying_resource()
            input_name = getattr(res, "input_name", "") or ""
            cs_name = getattr(res, "name", "") or cs.get_id()
            label = f"{sys_name} · {cs_name}"
            if input_name and input_name not in label:
                label += f" [{input_name}]"
            out.append(ControlOption(label=label, cs_id=cs.get_id(),
                                     controlstream=cs))
    out.sort(key=lambda o: 0 if "ptzControl" in o.label else 1)
    return out


def build_codec_context():
    """Create a fresh PyAV H.264 decoder context. Imported lazily so the
    file can be inspected without the [av] extra installed."""
    import av  # type: ignore

    ctx = av.codec.CodecContext.create("h264", "r")
    return ctx


# ---------------------------------------------------------------------------
# Initial-selection resolution
# ---------------------------------------------------------------------------


def _pick_initial(options: list, saved_id: Optional[str], env_id: Optional[str],
                  id_attr: str):
    """Resolve the initial selection: saved config id → env default id →
    first option. Returns the chosen option (or None when ``options`` is
    empty)."""
    by_id = {getattr(o, id_attr): o for o in options}
    if saved_id and saved_id in by_id:
        return by_id[saved_id]
    if env_id and env_id in by_id:
        return by_id[env_id]
    return options[0] if options else None


# ---------------------------------------------------------------------------
# PTZ control wiring
# ---------------------------------------------------------------------------


@dataclass
class PtzControl:
    """Live PTZ control surface plus the last-command/last-status display
    strings the GUI binds to."""
    controlstream: ControlStream
    last_command: str = "(none)"
    last_status: str = "(no status yet)"
    commands_sent: int = 0
    status_msgs: int = 0


def setup_ptz_control(cs: ControlStream) -> PtzControl:
    """Wire a discovered ControlStream for live PTZ driving.

    Forces its command_format to ``application/swe+json`` (Axis only parses
    commands in that wire form; ``application/json`` returns 500 on this
    driver), initializes MQTT, derives the status topic, and returns a
    `PtzControl` for the GUI to drive.
    """
    # Override the discovered JSONCommandSchema with the swe+json variant
    # so init_mqtt() picks the /swe-json topic suffix — the only format
    # the Axis ptzControl driver actually accepts. Use model_construct
    # to skip the (otherwise-required) `encoding` / `record_schema` fields
    # we don't need just to drive the topic suffix.
    cs._underlying_resource.command_schema = SWEJSONCommandSchema.model_construct(
        command_format="application/swe+json",
    )
    # Rebuild the topic strings now that the command_format changed.
    # _status_topic was set in __init__ before we overrode the schema, so
    # re-derive both — command topic via init_mqtt, status topic via the
    # explicit helper.
    cs.set_connection_mode(StreamableModes.BIDIRECTIONAL)
    cs.initialize()
    cs._status_topic = cs.get_mqtt_status_topic()

    logging.info("[PTZ] command topic: %s", cs._topic)
    logging.info("[PTZ] status topic:  %s", cs._status_topic)

    return PtzControl(controlstream=cs)


def send_ptz(ptz: Optional[PtzControl], **fields: float) -> None:
    """Publish one PTZ command. ``fields`` is a single-key dict like
    ``{"rpan": 5.0}`` per the DataChoice schema — passing more than one
    key still works on the wire but only the first option in the choice
    is meaningful to the Axis driver. No-ops when no control stream is
    selected."""
    if ptz is None or not fields:
        return
    payload = json.dumps(fields).encode("utf-8")
    cs = ptz.controlstream
    try:
        cs.publish_command(payload)
    except Exception as exc:  # noqa: BLE001
        ptz.last_command = f"ERROR: {exc}"
        logging.error("PTZ publish failed: %s", exc)
        return
    ptz.commands_sent += 1
    ptz.last_command = ", ".join(f"{k}={v}" for k, v in fields.items())
    logging.info("[PTZ] sent %s -> %s", ptz.last_command, cs._topic)


def attach_ptz_status_subscriber(ptz: PtzControl) -> None:
    """Subscribe to the PTZ status topic and store the latest payload on
    `ptz.last_status` so the GUI can show command acks live."""
    cs = ptz.controlstream
    if cs._mqtt_client is None:
        return

    def _on_status(client, userdata, msg):
        ptz.status_msgs += 1
        try:
            decoded = msg.payload.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            ptz.last_status = repr(msg.payload[:80])
            return
        # Pull just the keys the operator cares about. Slicing the raw
        # JSON lands mid-token on long payloads (e.g. chops the 's' off
        # "statusCode"), so parse properly first and fall back to a
        # head-truncated raw view only when parsing fails.
        try:
            obj = json.loads(decoded)
            code = obj.get("statusCode") or obj.get("currentStatus") or "?"
            cmd_id = obj.get("command@id") or obj.get("commandId") or obj.get("id") or ""
            exec_time = obj.get("executionTime")
            if isinstance(exec_time, list) and exec_time:
                exec_time = exec_time[-1]
            parts = [f"statusCode={code}"]
            if cmd_id:
                parts.append(f"cmd={cmd_id}")
            if exec_time:
                parts.append(f"at={exec_time}")
            ptz.last_status = "  ".join(parts)
        except (ValueError, TypeError):
            ptz.last_status = decoded[:120] + ("…" if len(decoded) > 120 else "")

    cs._mqtt_client.subscribe(cs._status_topic, msg_callback=_on_status)


def switch_control(ptz_holder: Holder, option: Optional[ControlOption]) -> None:
    """Tear down the currently-wired PTZ control (if any) and bring up the
    one named by ``option``. Called from the GUI thread on dropdown change
    — paho sub/unsubscribe are thread-safe."""
    old: Optional[PtzControl] = ptz_holder.current  # type: ignore[assignment]
    if old is not None and old.controlstream._mqtt_client is not None:
        try:
            old.controlstream._mqtt_client.unsubscribe(old.controlstream._status_topic)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to unsubscribe old PTZ status topic: %s", exc)

    if option is None:
        ptz_holder.current = None
        return

    ptz = setup_ptz_control(option.controlstream)
    attach_ptz_status_subscriber(ptz)
    ptz_holder.current = ptz


# ---------------------------------------------------------------------------
# MQTT → frame dispatch
# ---------------------------------------------------------------------------


def make_msg_callback(cam: CameraStream):
    """Build a paho-mqtt message callback for one camera.

    Captures `cam` in the closure so we don't need a topic→camera lookup
    inside the callback hot path. The callback runs on paho's network
    thread, so it must not touch tkinter — we only decode here and push
    the resulting RGB ndarray onto `cam.latest_frame` for the GUI thread
    to consume.
    """
    def _on_msg(client, userdata, msg):
        cam.nals_received += 1
        try:
            record = cam.datastream.decode_observation(msg.payload)
        except Exception as exc:  # noqa: BLE001
            cam.last_error = f"swe-binary decode: {exc}"
            return

        nal_bytes = record.get("img")
        if not nal_bytes:
            return

        try:
            import av  # type: ignore
            packet = av.Packet(nal_bytes)
            frames = cam.codec_ctx.decode(packet)
        except Exception as exc:  # noqa: BLE001
            # PyAV can throw on malformed NALs or before SPS/PPS lands —
            # capture and continue, the next keyframe usually recovers.
            cam.last_error = f"h264 decode: {exc}"
            return

        for frame in frames:
            try:
                rgb = frame.to_ndarray(format="rgb24")
            except Exception as exc:  # noqa: BLE001
                cam.last_error = f"frame->ndarray: {exc}"
                continue
            cam.frames_decoded += 1
            cam.latest_frame.append(rgb)

    return _on_msg


def subscribe_video(option: VideoOption) -> CameraStream:
    """Resolve a `VideoOption` to a freshly-wired `CameraStream` and start
    its MQTT subscription. State (codec context, counters) is brand new so
    a switched-to stream starts clean rather than inheriting the previous
    camera's error text."""
    cam = CameraStream(label=option.label, ds_id=option.ds_id)
    ds = option.datastream
    try:
        cam.codec_ctx = build_codec_context()
    except ImportError:
        cam.last_error = (
            "PyAV not installed — `uv pip install -e '.[av]'` to enable "
            "live H.264 decode")
        return cam
    cam.datastream = ds

    # PULL is the only mode that actually calls subscribe() inside
    # Datastream.start(); without this the start path tries to spawn an
    # async write task instead.
    ds.set_connection_mode(StreamableModes.PULL)
    ds.initialize()

    logging.info("[%s] subscribing to MQTT topic: %s", cam.label, ds._topic)
    # We want our custom callback, not the default deque-append, so call
    # subscribe directly rather than ds.start().
    ds._mqtt_client.subscribe(ds._topic, msg_callback=make_msg_callback(cam))
    return cam


def switch_video(cam_holder: Holder, option: Optional[VideoOption]) -> None:
    """Unsubscribe the currently-streaming datastream (if any) and subscribe
    the one named by ``option``. Called from the GUI thread on dropdown
    change."""
    old: Optional[CameraStream] = cam_holder.current  # type: ignore[assignment]
    if old is not None and old.datastream is not None:
        try:
            old.datastream._mqtt_client.unsubscribe(old.datastream._topic)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to unsubscribe old video topic: %s", exc)

    cam_holder.current = subscribe_video(option) if option is not None else None


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------


def _build_ptz_panel(parent, ptz_holder: Holder, status_var, cmd_var):
    """Build the PTZ control row. The directional buttons read the *current*
    control stream out of `ptz_holder` each time they fire, so they keep
    working after a live control-stream switch."""
    import tkinter as tk

    frame = tk.Frame(parent, padx=8, pady=8, borderwidth=1, relief="groove")
    tk.Label(frame, text="PTZ controls (assume a PTZ rig)",
             font=("Helvetica", 11, "bold")).grid(
        row=0, column=0, columnspan=8, sticky="w")

    # Row of directional / zoom buttons. Pan and tilt are *relative* so the
    # operator can nudge without knowing the current absolute pose; zoom
    # uses the relative `rzoom` knob for the same reason. Each lambda reads
    # ptz_holder.current at click time — not a captured PtzControl.
    btn_specs = [
        ("◀ pan-", lambda: send_ptz(ptz_holder.current, rpan=-PTZ_PAN_STEP)),
        ("pan+ ▶", lambda: send_ptz(ptz_holder.current, rpan=+PTZ_PAN_STEP)),
        ("▲ tilt+", lambda: send_ptz(ptz_holder.current, rtilt=+PTZ_TILT_STEP)),
        ("tilt- ▼", lambda: send_ptz(ptz_holder.current, rtilt=-PTZ_TILT_STEP)),
        ("zoom −", lambda: send_ptz(ptz_holder.current, rzoom=-PTZ_ZOOM_STEP)),
        ("zoom +", lambda: send_ptz(ptz_holder.current, rzoom=+PTZ_ZOOM_STEP)),
        ("⌂ home", lambda: send_ptz(ptz_holder.current, pan=0.0)),
    ]
    for col, (label, cb) in enumerate(btn_specs):
        tk.Button(frame, text=label, width=9, command=cb).grid(
            row=1, column=col, padx=2, pady=4)

    tk.Label(frame, textvariable=cmd_var, font=("Helvetica", 10),
             fg="#1b8a3a").grid(row=2, column=0, columnspan=8, sticky="w")
    tk.Label(frame, textvariable=status_var, font=("Helvetica", 9),
             fg="#555", wraplength=720, justify="left").grid(
        row=3, column=0, columnspan=8, sticky="w")
    return frame


def _schedule_ptz_auto(root, ptz_holder: Holder) -> None:
    """Fire a small scripted PTZ sequence so the example can be verified
    headlessly. Each step is debounced so command/status traffic doesn't
    pile up on the broker."""
    steps = [
        ("nudge pan +", lambda: send_ptz(ptz_holder.current, rpan=+PTZ_PAN_STEP)),
        ("nudge pan -", lambda: send_ptz(ptz_holder.current, rpan=-PTZ_PAN_STEP)),
        ("nudge tilt -", lambda: send_ptz(ptz_holder.current, rtilt=-PTZ_TILT_STEP)),
        ("nudge tilt +", lambda: send_ptz(ptz_holder.current, rtilt=+PTZ_TILT_STEP)),
        ("home", lambda: send_ptz(ptz_holder.current, pan=0.0)),
    ]
    delay_ms = 1200
    for i, (label, cb) in enumerate(steps):
        def _fire(label=label, cb=cb):
            logging.info("[PTZ-AUTO] %s", label)
            cb()
        root.after(800 + i * delay_ms, _fire)


def run_gui(video_options: list[VideoOption],
            control_options: list[ControlOption],
            cam_holder: Holder,
            ptz_holder: Holder,
            stop_after: float = 0.0) -> int:
    """Block on a tkinter window: one video panel, a video-datastream
    dropdown, a control-stream dropdown, and the PTZ control row. Switching
    a dropdown re-subscribes live and writes the new pair to the config
    file. Returns process exit code (0 if any frame decoded, else 2)."""
    try:
        import tkinter as tk
        from tkinter import ttk

        from PIL import Image, ImageTk  # type: ignore
    except ImportError as exc:
        print("GUI needs Pillow + tkinter:", exc)
        print("Install via:  uv pip install -e '.[av]'")
        return 2

    root = tk.Tk()
    root.title("OSH camera — live MQTT video (swe+binary) + PTZ")
    container = tk.Frame(root, padx=12, pady=12)
    container.pack()

    tk.Label(container,
             text=("Live frames decoded from MQTT swe-binary messages. "
                   "Pick a datastream / control stream below — selections "
                   "round-trip through the config file."),
             font=("Helvetica", 11, "bold")).grid(
        row=0, column=0, columnspan=2, pady=(0, 10))

    # --- selection row: two dropdowns -------------------------------------
    sel = tk.Frame(container)
    sel.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 8))

    video_by_label = {o.label: o for o in video_options}
    control_by_label = {o.label: o for o in control_options}

    tk.Label(sel, text="Video datastream:").grid(row=0, column=0, sticky="w", padx=(0, 6))
    video_var = tk.StringVar()
    video_box = ttk.Combobox(sel, textvariable=video_var, state="readonly",
                             width=44, values=list(video_by_label.keys()))
    video_box.grid(row=0, column=1, sticky="w", pady=2)

    tk.Label(sel, text="Control stream:").grid(row=1, column=0, sticky="w", padx=(0, 6))
    control_var = tk.StringVar()
    control_box = ttk.Combobox(sel, textvariable=control_var, state="readonly",
                               width=44, values=list(control_by_label.keys()))
    control_box.grid(row=1, column=1, sticky="w", pady=2)

    # Reflect the already-resolved initial selection in the widgets.
    if cam_holder.current is not None:
        video_var.set(cam_holder.current.label)  # type: ignore[union-attr]
    elif not video_options:
        video_var.set("(no swe+binary video datastreams found)")
    if ptz_holder.current is not None:
        cur_cs_id = ptz_holder.current.controlstream.get_id()  # type: ignore[union-attr]
        for o in control_options:
            if o.cs_id == cur_cs_id:
                control_var.set(o.label)
                break
    elif not control_options:
        control_var.set("(no control streams found)")

    def _current_ids() -> tuple[Optional[str], Optional[str]]:
        v = cam_holder.current.ds_id if cam_holder.current is not None else None  # type: ignore[union-attr]
        c = (ptz_holder.current.controlstream.get_id()  # type: ignore[union-attr]
             if ptz_holder.current is not None else None)
        return v, c

    def _on_video_selected(_event=None):
        option = video_by_label.get(video_var.get())
        switch_video(cam_holder, option)
        v, c = _current_ids()
        save_selection(v, c)

    def _on_control_selected(_event=None):
        option = control_by_label.get(control_var.get())
        switch_control(ptz_holder, option)
        v, c = _current_ids()
        save_selection(v, c)

    video_box.bind("<<ComboboxSelected>>", _on_video_selected)
    control_box.bind("<<ComboboxSelected>>", _on_control_selected)

    # --- video panel ------------------------------------------------------
    target_w = 640
    panel = tk.Frame(container)
    panel.grid(row=2, column=0, columnspan=2)
    img_label = tk.Label(panel, borderwidth=2, relief="solid",
                         width=target_w // 8, height=target_w // 14)
    img_label.grid(row=0, column=0, pady=(4, 4))
    stats_label = tk.Label(panel, text="(waiting for first frame)",
                           font=("Helvetica", 10), fg="#555")
    stats_label.grid(row=1, column=0)

    # --- PTZ control row --------------------------------------------------
    cmd_var = tk.StringVar(value="Last command: (none)")
    status_var = tk.StringVar(value="Last status: (none)")
    ptz_panel = _build_ptz_panel(container, ptz_holder, status_var, cmd_var)
    ptz_panel.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(12, 0))

    # --- Stop button ------------------------------------------------------
    # Quitting the mainloop drops out of run_gui into main()'s finally
    # block, which disconnects MQTT cleanly — same path as the window-close
    # handler, so closing the window and clicking Stop behave identically.
    tk.Button(container, text="■ Stop", width=12, fg="#b1331e",
              command=root.quit).grid(row=4, column=0, columnspan=2,
                                      pady=(12, 0))

    # Keep a strong reference on the root so tkinter doesn't GC the
    # PhotoImages between ticks.
    photo_refs: list = []
    root._photo_refs = photo_refs  # type: ignore[attr-defined]

    start_wall = time.monotonic()

    def tick():
        cam: Optional[CameraStream] = cam_holder.current  # type: ignore[assignment]
        if cam is None:
            stats_label.config(text="(no video datastream selected)", fg="#555")
        elif cam.last_error and cam.frames_decoded == 0:
            stats_label.config(text=f"{cam.label} — {cam.last_error}", fg="#b1331e")
        else:
            if cam.latest_frame:
                rgb = cam.latest_frame.popleft()
                h, w = rgb.shape[:2]
                scale = min(1.0, target_w / w)
                new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
                photo = ImageTk.PhotoImage(Image.fromarray(rgb).resize(new_size))
                img_label.config(image=photo, width=new_size[0], height=new_size[1])
                photo_refs.append(photo)
                # Trim the cache so we don't grow without bound.
                if len(photo_refs) > 4:
                    del photo_refs[:2]
            err_note = f"  ·  last error: {cam.last_error}" if cam.last_error else ""
            stats_label.config(
                text=(f"{cam.label}  ·  nals={cam.nals_received}  "
                      f"frames={cam.frames_decoded}{err_note}"),
                fg=("#1b8a3a" if cam.frames_decoded > 0 else "#555"))

        ptz: Optional[PtzControl] = ptz_holder.current  # type: ignore[assignment]
        if ptz is not None:
            cmd_var.set(f"Last command: {ptz.last_command}   "
                        f"(sent={ptz.commands_sent})")
            status_var.set(f"Last status [{ptz.status_msgs}]: {ptz.last_status}")
        else:
            cmd_var.set("Last command: (no control stream selected)")
            status_var.set("Last status: —")

        if stop_after > 0 and (time.monotonic() - start_wall) >= stop_after:
            root.quit()
        else:
            root.after(40, tick)

    if PTZ_AUTO and ptz_holder.current is not None:
        _schedule_ptz_auto(root, ptz_holder)

    root.after(40, tick)
    root.protocol("WM_DELETE_WINDOW", root.quit)
    root.mainloop()
    root.destroy()

    cam = cam_holder.current  # type: ignore[assignment]
    return 0 if (cam is not None and cam.frames_decoded > 0) else 2


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("OSHC_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    print(f"Node:    http://{HOST}:{HTTP_PORT}  (MQTT :{MQTT_PORT})")
    print(f"Config:  {CONFIG_PATH}")

    osh, node = connect_and_discover()
    video_options = discover_video_options(node)
    control_options = discover_control_options(node)
    print(f"Discovered {len(video_options)} video datastream(s), "
          f"{len(control_options)} control stream(s).")

    saved = load_selection()
    initial_video = _pick_initial(
        video_options, saved.get("video_datastream_id"), ENV_VIDEO_DS_ID, "ds_id")
    initial_control = _pick_initial(
        control_options, saved.get("control_stream_id"), PTZ_CS_ID, "cs_id")

    # Resolve the initial selection BEFORE the window / PTZ_AUTO script so a
    # headless run actually has a stream to drive.
    cam_holder = Holder()
    ptz_holder = Holder()
    switch_video(cam_holder, initial_video)
    switch_control(ptz_holder, initial_control)
    # Persist the resolved pair so the config file reflects what's live even
    # on a first run with no prior config.
    if initial_video is not None or initial_control is not None:
        save_selection(
            initial_video.ds_id if initial_video else None,
            initial_control.cs_id if initial_control else None)

    if cam_holder.current is not None:
        print(f"  video:   {cam_holder.current.label} "  # type: ignore[union-attr]
              f"(topic {cam_holder.current.datastream._topic})")  # type: ignore[union-attr]
    else:
        print("  video:   (none selected)")
    if ptz_holder.current is not None:
        cs = ptz_holder.current.controlstream  # type: ignore[union-attr]
        print(f"  control: {cs.get_id()} (cmd {cs._topic}, status {cs._status_topic})")
    else:
        print("  control: (none selected)")

    # Small grace period so SPS/PPS NALs land before the GUI opens — not
    # strictly required (the decoder catches up at the next keyframe) but
    # it makes the first second of the demo look better.
    time.sleep(1.0)

    try:
        rc = run_gui(video_options, control_options,
                     cam_holder, ptz_holder, stop_after=RUN_SECS)
    finally:
        # paho-mqtt's network loop is daemonized via loop_start(), so
        # process exit cleans it up — but disconnect cleanly anyway so the
        # broker sees a graceful close instead of a TCP RST.
        client = node.get_mqtt_client()
        if client is not None:
            try:
                client.stop()
                client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    print("\nSummary:")
    cam = cam_holder.current
    if cam is None:
        print("  video:   (none selected)")
    elif cam.last_error and cam.frames_decoded == 0:
        print(f"  video:   {cam.label} ({cam.ds_id}): ERROR — {cam.last_error}")
    else:
        print(f"  video:   {cam.label} ({cam.ds_id}): "
              f"{cam.nals_received} NALs, {cam.frames_decoded} frames decoded"
              f"{'  (' + cam.last_error + ')' if cam.last_error else ''}")
    ptz = ptz_holder.current
    if ptz is not None:
        print(f"  control: {ptz.controlstream.get_id()}: "
              f"{ptz.commands_sent} commands sent, "
              f"{ptz.status_msgs} status messages received "
              f"(last: {ptz.last_status})")
    return rc


if __name__ == "__main__":
    # Silence noisy paho debug logging unless the user explicitly cranks
    # the level via OSHC_LOG_LEVEL.
    logging.getLogger("paho").setLevel(logging.WARNING)
    # Ensure no leftover background threads hold the process up.
    sys.exit(main())
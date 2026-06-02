# Live MQTT video viewer demo

`axis_video_mqtt_stream.py` connects to an OpenSensorHub (OSH) node, discovers
its video datastreams and control streams, and shows **one** live video panel
with two dropdowns:

- **Video datastream** — every `application/swe+binary` H.264 video source on
  the node.
- **Control stream** — every control stream on the node (the PTZ buttons
  assume a pan/tilt/zoom rig).

Switching a dropdown re-subscribes live — no restart. Both selections
round-trip through `axis_video_config.json` (written next to the script), so
the next launch restores them.

---

## 1. Set up a Python environment

The library targets **Python 3.12–3.14** (`requires-python = "<4.0,>=3.12"`).
The demo needs the optional **`[av]`** extra (PyAV for H.264 decode + Pillow)
and **tkinter** for the window.

### With `uv` (recommended)

From the repo root:

```bash
uv sync --all-extras          # installs the library + av/pillow + dev tools
uv run python examples/axis_video_mqtt_stream.py
```

To install only what the demo needs:

```bash
uv pip install -e ".[av]"
uv run python examples/axis_video_mqtt_stream.py
```

### With plain `pip` / venv

```bash
python3.12 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[av]"
python examples/axis_video_mqtt_stream.py
```

### tkinter

tkinter ships with the python.org installer and most distro Python packages.
On Homebrew or pyenv builds you may need the system `tcl-tk` package:

```bash
brew install tcl-tk               # macOS / Homebrew
sudo apt install python3-tk       # Debian / Ubuntu
```

If PyAV, Pillow, or tkinter are missing the script prints an install hint and
exits instead of crashing.

---

## 2. What the OSH node must provide

The demo is read-mostly: it subscribes to a video datastream over MQTT and
(optionally) publishes PTZ commands. For data to show up, the node needs:

### Required — for video

1. **MQTT enabled** on the node. The demo connects to the broker on
   `localhost:1883` by default (CS API Part 3 Pub/Sub).
2. **At least one video datastream** whose observation schema is
   `application/swe+binary` and exposes an **`img`** block member carrying
   raw H.264 NAL units. This is the Axis/Amcrest driver convention; the demo
   filters for exactly this shape (`is_swe_binary_video`) and ignores other
   datastreams.
3. The server must support the **`:data/<format>` format subtopic** added in
   CS API Part 3 — the demo subscribes to
   `…/datastreams/<id>/observations:data/swe-binary`, not bare `:data`.
4. The datastream must actually be **producing observations** — i.e. the
   camera/RTP feed is connected and frames are buffered. An idle datastream
   discovers fine but the panel stays on "waiting for first frame".

### Optional — for PTZ control

5. A **control stream** for the PTZ rig. The demo's buttons send relative
   pan/tilt/zoom commands (`rpan`, `rtilt`, `rzoom`) as
   `application/swe+json`, published to
   `…/controlstreams/<id>/commands:data/swe-json`, and listen for acks on
   `…/controlstreams/<id>/status:data/json`. A non-PTZ control stream can be
   selected but the buttons won't mean anything to it.

A typical source is the OSH Axis video driver (`osh-addons`), which registers
both the `video1` swe+binary datastream and a `ptzControl` control stream.

---

## 3. Running it

```bash
uv run python examples/axis_video_mqtt_stream.py
```

On launch it prints what it discovered and which streams it selected, e.g.:

```
Discovered 1 video datastream(s), 1 control stream(s).
  video:   Office Axis Video Camera · … - video1 (topic …/observations:data/swe-binary)
  control: 02hqdbu6j4f0 (cmd …/commands:data/swe-json, status …/status:data/json)
```

Use the dropdowns to switch streams, the PTZ buttons to drive the rig, and the
**■ Stop** button (or closing the window) to exit cleanly.

---

## 4. Configuration

The **initial** video / control selection resolves in this order:

1. `axis_video_config.json` (last saved selection),
2. environment defaults (`OSHC_AXIS_CAMERAS` first entry / `OSHC_PTZ_CS_ID`),
3. the first discovered entry.

On startup the resolved pair is written back, so a hand-edited config pointing
at a stream no longer on the node is silently rewritten to the fallback (valid
ids are left untouched). The file is git-ignored — it's runtime state.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `OSHC_AXIS_HOST` | `localhost` | Server hostname / IP |
| `OSHC_AXIS_PORT` | `8282` | HTTP API port |
| `OSHC_AXIS_MQTT_PORT` | `1883` | MQTT broker port |
| `OSHC_AXIS_USER` / `OSHC_AXIS_PASS` | _(none)_ | HTTP Basic-Auth credentials |
| `OSHC_AXIS_CAMERAS` | _(none)_ | `Label:ds_id[,…]` — only the **first** id is used as the initial video default |
| `OSHC_PTZ_CS_ID` | _(none)_ | Control-stream id to pre-select |
| `OSHC_AXIS_CONFIG` | _beside script_ | Path to the selection config JSON |
| `OSHC_PTZ_PAN_STEP` / `OSHC_PTZ_TILT_STEP` / `OSHC_PTZ_ZOOM_STEP` | `5` / `2` / `1` | Relative step sizes for the PTZ buttons |
| `OSHC_AXIS_RUN_SECS` | `0` | Auto-exit after N seconds (`0` = run until closed); useful for headless checks |
| `OSHC_PTZ_AUTO` | _(off)_ | Fire a scripted PTZ sequence on launch (for headless verification) |
| `OSHC_LOG_LEVEL` | `INFO` | Logging verbosity |

Example — point at a remote node with credentials and a 30-second timed run:

```bash
OSHC_AXIS_HOST=10.0.0.5 OSHC_AXIS_USER=admin OSHC_AXIS_PASS=secret \
OSHC_AXIS_RUN_SECS=30 uv run python examples/axis_video_mqtt_stream.py
```

---

## 5. Troubleshooting

- **"no swe+binary video datastreams found"** — the node has no datastream
  with a `swe+binary` schema + `img` block member, or discovery failed. Check
  the node has a video driver registered and is reachable on the HTTP port.
- **Panel stuck on "waiting for first frame"** — datastream exists but no
  frames are flowing (camera/RTP feed offline). The stats line shows
  `nals=0`; the next H.264 keyframe usually recovers a stream that just
  started.
- **`h264 decode` errors that clear themselves** — PyAV throws on inter-frames
  before the first SPS/PPS keyframe lands; this is expected and self-recovers.
- **PTZ buttons do nothing / 500 errors** — the selected control stream isn't
  a PTZ rig, or the driver rejects `swe+json` commands. The Axis `ptzControl`
  driver only accepts `application/swe+json`, which is what the demo sends.
- **GUI won't open** — install tkinter (see §1).

See the module docstring in `axis_video_mqtt_stream.py` for more detail.

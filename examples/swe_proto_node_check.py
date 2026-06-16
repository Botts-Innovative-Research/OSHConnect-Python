#!/usr/bin/env python
#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/6/11
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Diagnostic check of ``application/swe+proto`` against a live OSH node.

Runs every swe+proto-advertising datastream on the node through a staged
pipeline and reports, per stage, whether the failure (if any) is on the
node side or the client side:

0. **advertised**    — the datastream lists ``application/swe+proto`` in
   its ``formats`` array. A datastream that *serves* the format on its
   schema endpoint but omits it from ``formats`` is still tested, with
   this stage marked FAIL (node-side advertisement regression).
1. **schema-fetch**  — ``GET /datastreams/{id}/schema?obsFormat=application/swe+proto``.
   An HTTP error here is node-side (consys-proto module missing, format
   not registered, or a server exception).
2. **schema-parse**  — parse the JSON envelope
   (``{"obsFormat", "messageType", "fileDescriptorSet": <base64>}``) into
   `SWEProtobufDatastreamRecordSchema` and build a `SWEProtobufCodec`
   from the delivered ``FileDescriptorSet``. A failure here is a contract
   drift between the node's schema document and this client. The decoded
   ``.proto`` source is written to the log file for inspection.
3. **http-fetch**    — ``GET /datastreams/{id}/observations?f=application/swe+proto``.
4. **decode**        — split the response into varint-length-delimited
   frames (the node writes each observation with protobuf
   ``writeDelimitedTo``) and decode each with the codec. Decoded
   observations are logged as pretty JSON; a frame that fails to decode
   gets a hex dump in the log file so the bytes can be diffed against
   the descriptor.
5. **cross-check**   — fetch the same observations as
   ``application/om+json`` and compare result values field-by-field
   (matched by ``phenomenonTime``). A mismatch means the proto wire type
   or field mapping has drifted from the JSON truth.
6. **mqtt**          — subscribe to the CS API Part 3
   ``…/observations:data/swe-proto`` topic, collect a few live messages,
   and decode them the same way. Requires the node's MQTT service
   (default broker port 1883). Skippable via ``OSHC_PROTO_MQTT_SECS=0``.

Console output is a concise INFO narrative plus decoded samples; the log
file (``swe_proto_node_check.log`` beside this script by default) gets
DEBUG detail — full tracebacks, raw hex previews, and the rendered
``.proto`` source — so a failed run is attributable without re-running.

Defaults
--------
* Node:  ``http://localhost:8282/sensorhub/api`` (HTTP)
*        ``localhost:1883`` (MQTT broker on the same host)

Override with:

* ``OSHC_PROTO_HOST``      — server hostname/IP (default ``localhost``).
* ``OSHC_PROTO_PORT``      — HTTP API port (default ``8282``).
* ``OSHC_PROTO_MQTT_PORT`` — MQTT broker port (default ``1883``).
* ``OSHC_PROTO_USER`` / ``OSHC_PROTO_PASS`` — Basic-Auth credentials, if any.
* ``OSHC_PROTO_OBS_COUNT`` — observations to request per HTTP fetch (default ``5``).
* ``OSHC_PROTO_MQTT_SECS`` — seconds to listen per datastream on MQTT
  (default ``10``; ``0`` skips the MQTT stage).
* ``OSHC_PROTO_MQTT_MSGS`` — stop listening early after this many
  messages (default ``5``).
* ``OSHC_PROTO_LOG``       — log file path (default beside this script).

Run
---
    uv run python examples/swe_proto_node_check.py

Exit codes: ``0`` all stages passed (skips allowed), ``1`` at least one
stage failed, ``2`` node unreachable / nothing to test.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import requests

from oshconnect import OSHConnect
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.node import Node
from oshconnect.resources.base import StreamableModes
from oshconnect.resources.datastream import Datastream
from oshconnect.schema_datamodels import SWEProtobufDatastreamRecordSchema
from oshconnect.swe_protobuf import SWEProtobufCodec

SWE_PROTO = "application/swe+proto"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HOST = os.environ.get("OSHC_PROTO_HOST", "localhost")
HTTP_PORT = int(os.environ.get("OSHC_PROTO_PORT", "8282"))
MQTT_PORT = int(os.environ.get("OSHC_PROTO_MQTT_PORT", "1883"))
USER = os.environ.get("OSHC_PROTO_USER") or None
PASS = os.environ.get("OSHC_PROTO_PASS") or None
OBS_COUNT = int(os.environ.get("OSHC_PROTO_OBS_COUNT", "5"))
MQTT_SECS = float(os.environ.get("OSHC_PROTO_MQTT_SECS", "10"))
MQTT_MSGS = int(os.environ.get("OSHC_PROTO_MQTT_MSGS", "5"))
LOG_PATH = Path(
    os.environ.get("OSHC_PROTO_LOG", "")
    or str(Path(__file__).with_name("swe_proto_node_check.log")))

log = logging.getLogger("swe_proto_check")


def setup_logging() -> None:
    """Console = concise INFO narrative; log file = DEBUG forensics.

    The file handler gets everything (tracebacks, hex dumps, the rendered
    ``.proto`` source) so a failed run can be diagnosed without re-running
    with a different verbosity.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)-7s %(message)s"))
    root.addHandler(console)

    filehandler = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
    filehandler.setLevel(logging.DEBUG)
    filehandler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root.addHandler(filehandler)

    # paho/urllib3 DEBUG spam stays out of the file unless explicitly wanted.
    logging.getLogger("paho").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("oshconnect").setLevel(logging.INFO)


def hexdump(data: bytes, limit: int = 64) -> str:
    """Short hex preview of a payload for the log file."""
    head = data[:limit]
    body = " ".join(f"{b:02x}" for b in head)
    more = f" … (+{len(data) - limit} bytes)" if len(data) > limit else ""
    return f"[{len(data)} bytes] {body}{more}"


# ---------------------------------------------------------------------------
# Varint-delimited framing
# ---------------------------------------------------------------------------


def split_delimited(buf: bytes) -> list[bytes]:
    """Split a buffer of protobuf varint-length-delimited messages.

    The node emits each observation with ``Message.writeDelimitedTo`` (see
    ``ObsBindingProto.java``), so both the HTTP batch body and each MQTT
    payload are ``<varint length><message bytes>`` frames back to back.

    :raises ValueError: on a truncated varint or a frame that runs past
        the end of the buffer — both indicate corruption in transit or a
        framing change on the node side.
    """
    frames: list[bytes] = []
    i = 0
    while i < len(buf):
        length = 0
        shift = 0
        start = i
        while True:
            if i >= len(buf):
                raise ValueError(
                    f"truncated varint length prefix at offset {start}")
            byte = buf[i]
            i += 1
            length |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
            if shift > 35:
                raise ValueError(
                    f"varint length prefix at offset {start} exceeds 5 bytes "
                    "— payload is probably not delimited protobuf")
        if i + length > len(buf):
            raise ValueError(
                f"frame at offset {start} declares {length} bytes but only "
                f"{len(buf) - i} remain — truncated response?")
        frames.append(buf[i:i + length])
        i += length
    return frames


def sniff_json(payload: bytes) -> Optional[Any]:
    """Return the parsed document if ``payload`` is actually JSON text.

    The single most diagnostic failure mode: asking for swe+proto and
    getting JSON back means the node ignored (or doesn't implement)
    content negotiation for that path — a node-side break, not a codec
    bug. Detect it explicitly instead of letting the protobuf parser
    report a generic "wire format corrupt".
    """
    head = payload.lstrip()[:1]
    if head not in (b"{", b"["):
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None


def decode_payload(codec: SWEProtobufCodec, payload: bytes) -> list[dict]:
    """Decode one wire payload into observation dicts (with envelope).

    Tries varint-delimited framing first (the node's ``writeDelimitedTo``
    form); falls back to treating the payload as a single bare message.
    Logs which framing succeeded at DEBUG so a framing change on the node
    is visible in the log file.
    """
    try:
        frames = split_delimited(payload)
        decoded = [codec.decode_with_envelope(f) for f in frames]
        log.debug("payload decoded as %d delimited frame(s)", len(frames))
        return decoded
    except Exception as delimited_exc:  # noqa: BLE001
        log.debug("delimited decode failed (%s); trying bare message",
                  delimited_exc)
        decoded = [codec.decode_with_envelope(payload)]
        log.debug("payload decoded as a single bare (non-delimited) message")
        return decoded


# ---------------------------------------------------------------------------
# Stage bookkeeping
# ---------------------------------------------------------------------------

STAGES = ("advertised", "schema-fetch", "schema-parse", "http-fetch",
          "decode", "cross-check", "mqtt")


@dataclass
class DatastreamReport:
    """Per-datastream stage outcomes: PASS / FAIL / SKIP per stage."""
    ds_id: str
    name: str
    stages: dict = field(default_factory=dict)

    def record(self, stage: str, ok: Optional[bool], note: str = "") -> None:
        self.stages[stage] = ("PASS" if ok else "FAIL") if ok is not None else "SKIP"
        if note:
            self.stages[stage] += f" ({note})"

    @property
    def failed(self) -> bool:
        return any(v.startswith("FAIL") for v in self.stages.values())


def log_failure(stage: str, ds_id: str, exc: Exception, hint: str) -> None:
    """One ERROR line on the console with the attribution hint; the full
    traceback goes to the log file at DEBUG."""
    log.error("[%s] stage %s FAILED: %s: %s — %s",
              ds_id, stage, type(exc).__name__, exc, hint)
    log.debug("traceback for %s/%s:\n%s", ds_id, stage,
              "".join(traceback.format_exception(exc)))


def http_hint(exc: Exception) -> str:
    """Attribute an HTTP-stage failure to node vs client."""
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "node unreachable (node side — is it running?)"
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        code = exc.response.status_code
        if code in (404, 405, 406):
            return (f"HTTP {code}: node does not serve {SWE_PROTO} here "
                    "(node side — consys-proto module missing or format "
                    "not registered for this resource)")
        if code >= 500:
            return f"HTTP {code}: server exception (node side — check node logs)"
        return f"HTTP {code} (likely node side)"
    return "transport error"


# ---------------------------------------------------------------------------
# Cross-check helpers
# ---------------------------------------------------------------------------


def _parse_iso(ts: str) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp, tolerating the >6-digit fractional
    seconds protobuf ``Timestamp.ToJsonString`` can emit (nanoseconds)."""
    if not isinstance(ts, str):
        return None
    text = ts.replace("Z", "+00:00")
    if "." in text:
        head, _, tail = text.partition(".")
        frac = tail[:-6]  # strip the +00:00 suffix
        text = f"{head}.{frac[:6].ljust(6, '0')}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _times_close(a: str, b: str, tol_s: float = 0.001) -> bool:
    da, db = _parse_iso(a), _parse_iso(b)
    if da is None or db is None:
        return False
    return abs((da - db).total_seconds()) <= tol_s


def values_match(proto_val: Any, json_val: Any) -> bool:
    """Field-level equivalence between the proto and om+json wire forms.

    Numbers compare with a relative tolerance that absorbs float32
    round-tripping; timestamp strings compare to within 1 ms (om+json
    carries millis, protobuf ``Timestamp`` carries nanos).
    """
    if isinstance(proto_val, bool) or isinstance(json_val, bool):
        return bool(proto_val) == bool(json_val)
    if isinstance(proto_val, (int, float)) and isinstance(json_val, (int, float)):
        return math.isclose(float(proto_val), float(json_val),
                            rel_tol=1e-6, abs_tol=1e-9)
    if isinstance(proto_val, str) and isinstance(json_val, str):
        return proto_val == json_val or _times_close(proto_val, json_val)
    if isinstance(proto_val, dict) and isinstance(json_val, dict):
        return all(k in json_val and values_match(v, json_val[k])
                   for k, v in proto_val.items())
    if isinstance(proto_val, (list, tuple)) and isinstance(json_val, (list, tuple)):
        return (len(proto_val) == len(json_val)
                and all(values_match(p, j) for p, j in zip(proto_val, json_val)))
    return proto_val == json_val


def cross_check(proto_obs: list[dict], json_obs: list[dict]) -> tuple[int, list[str]]:
    """Compare decoded proto observations against the om+json reference.

    Observations are matched by ``phenomenonTime`` (the two HTTP fetches
    race the live store, so the sets may only partially overlap). For each
    matched pair, every result field present in *both* wire forms must
    agree. The proto-only ``time`` result field (the SWE sampling-time
    component, which om+json folds into ``phenomenonTime``) is checked
    against the JSON ``phenomenonTime`` instead.

    :returns: ``(matched_count, mismatch_descriptions)``
    """
    json_by_time = {o["phenomenonTime"]: o for o in json_obs
                    if o.get("phenomenonTime")}

    def find_json_match(ptime: str) -> Optional[dict]:
        if ptime in json_by_time:
            return json_by_time[ptime]
        for jt, obs in json_by_time.items():  # ns-vs-ms precision tolerance
            if _times_close(ptime, jt):
                return obs
        return None

    matched = 0
    mismatches: list[str] = []
    for pobs in proto_obs:
        ptime = pobs.get("phenomenonTime", "")
        jobs = find_json_match(ptime)
        if jobs is None:
            continue
        matched += 1
        presult, jresult = pobs.get("result", {}), jobs.get("result", {})
        for key, pval in presult.items():
            if key in jresult:
                if not values_match(pval, jresult[key]):
                    mismatches.append(
                        f"@{ptime} result[{key!r}]: proto={pval!r} json={jresult[key]!r}")
            elif key == "time" and isinstance(pval, str):
                # om+json lifts the SWE time component out of `result`.
                if not _times_close(pval, jobs.get("phenomenonTime", "")):
                    mismatches.append(
                        f"@{ptime} result['time']={pval!r} disagrees with "
                        f"phenomenonTime={jobs.get('phenomenonTime')!r}")
            else:
                mismatches.append(
                    f"@{ptime} result[{key!r}] present in proto but absent "
                    "from om+json — field-name drift?")
    return matched, mismatches


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------


def fetch_proto_schema(api, ds_id: str) -> SWEProtobufDatastreamRecordSchema:
    """Stages 1+2: fetch and parse the swe+proto schema document."""
    resp = api.get_resource(
        APIResourceTypes.DATASTREAM, ds_id, APIResourceTypes.SCHEMA,
        params={"obsFormat": SWE_PROTO})
    resp.raise_for_status()
    return SWEProtobufDatastreamRecordSchema.from_sweproto_dict(resp.json())


def fetch_observations_raw(api, ds_id: str, fmt: str) -> requests.Response:
    """GET ``/datastreams/{id}/observations?f=<fmt>&limit=N``."""
    resp = api.get_resource(
        APIResourceTypes.DATASTREAM, ds_id, APIResourceTypes.OBSERVATION,
        params={"f": fmt, "limit": OBS_COUNT})
    resp.raise_for_status()
    return resp


def run_mqtt_stage(ds: Datastream, codec: SWEProtobufCodec,
                   report: DatastreamReport,
                   last_obs_time: Optional[str] = None) -> None:
    """Stage 6: live MQTT subscription to the ``:data/swe-proto`` topic.

    Collects raw payloads on paho's network thread and decodes them here
    afterwards, so a codec bug can't kill the network loop.
    """
    if MQTT_SECS <= 0:
        report.record("mqtt", None, "disabled via OSHC_PROTO_MQTT_SECS=0")
        return
    if ds._parent_node.get_mqtt_client() is None:
        # MQTT_SECS > 0 (checked above) yet no client — the broker was
        # unreachable at startup and main() fell back to HTTP-only.
        report.record("mqtt", False, "MQTT broker unreachable")
        return

    payloads: list[bytes] = []

    def _on_msg(client, userdata, msg):
        payloads.append(bytes(msg.payload))

    try:
        ds.set_connection_mode(StreamableModes.PULL)
        ds.initialize()
        topic = ds._topic
        log.info("[%s] MQTT: subscribing to %s for up to %.0fs "
                 "(or %d messages)", report.ds_id, topic, MQTT_SECS, MQTT_MSGS)
        ds._mqtt_client.subscribe(topic, msg_callback=_on_msg)
    except Exception as exc:  # noqa: BLE001
        log_failure("mqtt", report.ds_id, exc,
                    "subscription setup failed (client side — topic "
                    "construction or MQTT connection)")
        report.record("mqtt", False, "subscribe failed")
        return

    deadline = time.monotonic() + MQTT_SECS
    while time.monotonic() < deadline and len(payloads) < MQTT_MSGS:
        time.sleep(0.2)

    try:
        ds._mqtt_client.unsubscribe(topic)
    except Exception:  # noqa: BLE001
        pass

    if not payloads:
        # An idle producer is not a transport failure: if the latest stored
        # observation predates the listen window by a wide margin, the
        # stream simply isn't publishing right now (e.g. a static
        # sensor-location output) and silence is the expected outcome.
        last_dt = _parse_iso(last_obs_time) if last_obs_time else None
        if last_dt is not None:
            age = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
            if age > max(2 * MQTT_SECS, 30):
                log.warning("[%s] MQTT: no messages in %.0fs, but the stream "
                            "is idle anyway (last stored observation %.0fs "
                            "ago) — cannot exercise the live path",
                            report.ds_id, MQTT_SECS, age)
                report.record("mqtt", None,
                              f"stream idle (last obs {age:.0f}s ago)")
                return
        log.error("[%s] MQTT: no messages in %.0fs on %s while the stream "
                  "appears live — node side (is the consys-mqtt service "
                  "running and does it accept the swe-proto subtopic?)",
                  report.ds_id, MQTT_SECS, topic)
        report.record("mqtt", False, "no messages received")
        return

    decoded_count = 0
    for i, payload in enumerate(payloads):
        log.debug("[%s] MQTT payload %d: %s", report.ds_id, i, hexdump(payload))
        json_doc = sniff_json(payload)
        if json_doc is not None:
            log.error(
                "[%s] MQTT: node published JSON on the %s subtopic — the "
                "format token was ignored (node side: the consys-mqtt "
                "service did not apply %s content negotiation to the "
                "outbound observation stream). Payload:\n%s",
                report.ds_id, topic.rsplit(":", 1)[-1], SWE_PROTO,
                json.dumps(json_doc, indent=2)[:800])
            report.record("mqtt", False, "node sent JSON, not protobuf")
            return
        try:
            for obs in decode_payload(codec, payload):
                decoded_count += 1
                if decoded_count <= 3:
                    log.info("[%s] MQTT observation %d:\n%s", report.ds_id,
                             decoded_count, json.dumps(obs, indent=2, default=str))
        except Exception as exc:  # noqa: BLE001
            log_failure("mqtt", report.ds_id, exc,
                        "payload decode failed (contract drift — compare "
                        "hex dump in log file against the schema descriptor)")
            report.record("mqtt", False, f"decode failed on message {i}")
            return

    log.info("[%s] MQTT: decoded %d observation(s) from %d message(s)",
             report.ds_id, decoded_count, len(payloads))
    report.record("mqtt", True, f"{decoded_count} obs")


def check_datastream(node: Node, ds: Datastream,
                     advertised: bool = True) -> DatastreamReport:
    """Run all stages for one datastream and return its report."""
    api = node.get_api_helper()
    res = ds.get_resource()
    report = DatastreamReport(ds_id=res.ds_id, name=res.name or res.ds_id)
    log.info("=== datastream %s (%s) ===", report.ds_id, report.name)

    # -- 0: format advertisement ----------------------------------------------
    if advertised:
        report.record("advertised", True)
    else:
        log.error("[%s] %s missing from the datastream's `formats` list even "
                  "though the schema endpoint serves it — node side (the "
                  "consys-proto CustomObsFormat is not being reported on the "
                  "datastream resource)", report.ds_id, SWE_PROTO)
        report.record("advertised", False, "format not in `formats` list")

    # -- 1+2: schema fetch + parse ------------------------------------------
    try:
        schema = fetch_proto_schema(api, report.ds_id)
        report.record("schema-fetch", True)
    except (requests.exceptions.RequestException, ValueError) as exc:
        log_failure("schema-fetch", report.ds_id, exc, http_hint(exc))
        report.record("schema-fetch", False)
        for stage in STAGES[1:]:
            report.record(stage, None, "blocked by schema-fetch")
        return report

    try:
        codec = SWEProtobufCodec(schema)
        log.info("[%s] schema: message_type=%s result_fields=%s",
                 report.ds_id, schema.message_type, codec.result_field_names)
        log.debug("[%s] rendered .proto source:\n%s",
                  report.ds_id, schema.to_proto_source())
        report.record("schema-parse", True)
    except Exception as exc:  # noqa: BLE001
        log_failure("schema-parse", report.ds_id, exc,
                    "descriptor unusable (contract drift — node's schema "
                    "document doesn't match the client's expectations)")
        report.record("schema-parse", False)
        for stage in STAGES[2:]:
            report.record(stage, None, "blocked by schema-parse")
        return report

    # Cache the proto schema on the datastream so init_mqtt() derives the
    # :data/swe-proto subtopic (discovery prefers the swe+json schema).
    ds._underlying_resource.record_schema = schema

    # -- 3: HTTP fetch -------------------------------------------------------
    try:
        resp = fetch_observations_raw(api, report.ds_id, SWE_PROTO)
        raw = resp.content
        log.info("[%s] HTTP: %d bytes of %s", report.ds_id, len(raw),
                 resp.headers.get("Content-Type", "?"))
        log.debug("[%s] HTTP body: %s", report.ds_id, hexdump(raw, 128))
        report.record("http-fetch", True)
    except requests.exceptions.RequestException as exc:
        log_failure("http-fetch", report.ds_id, exc, http_hint(exc))
        report.record("http-fetch", False)
        for stage in ("decode", "cross-check"):
            report.record(stage, None, "blocked by http-fetch")
        run_mqtt_stage(ds, codec, report)
        return report

    # -- 4: decode -----------------------------------------------------------
    proto_obs: list[dict] = []
    if not raw:
        report.record("decode", None, "no stored observations")
        report.record("cross-check", None, "no stored observations")
    elif sniff_json(raw) is not None:
        log.error("[%s] decode: HTTP body is JSON despite f=%s — node side "
                  "(content negotiation fell back to the default format)",
                  report.ds_id, SWE_PROTO)
        report.record("decode", False, "node sent JSON, not protobuf")
        report.record("cross-check", None, "blocked by decode")
    else:
        try:
            frames = split_delimited(raw)
            log.info("[%s] decode: %d delimited frame(s): %s bytes",
                     report.ds_id, len(frames), [len(f) for f in frames])
            for i, frame in enumerate(frames):
                try:
                    proto_obs.append(codec.decode_with_envelope(frame))
                except Exception:  # noqa: BLE001
                    log.debug("[%s] frame %d hex: %s", report.ds_id, i,
                              hexdump(frame, 128))
                    raise
            for i, obs in enumerate(proto_obs[:3]):
                log.info("[%s] HTTP observation %d:\n%s", report.ds_id, i + 1,
                         json.dumps(obs, indent=2, default=str))
            report.record("decode", True, f"{len(proto_obs)} obs")
        except Exception as exc:  # noqa: BLE001
            log_failure("decode", report.ds_id, exc,
                        "wire bytes don't match the delivered descriptor "
                        "(either side — hex dump in log file; diff against "
                        "the rendered .proto source)")
            report.record("decode", False)
            report.record("cross-check", None, "blocked by decode")
            run_mqtt_stage(ds, codec, report)
            return report

        # -- 5: cross-check against om+json ----------------------------------
        try:
            json_resp = fetch_observations_raw(api, report.ds_id,
                                               "application/om+json")
            json_obs = json_resp.json().get("items", [])
            matched, mismatches = cross_check(proto_obs, json_obs)
            if mismatches:
                for m in mismatches:
                    log.error("[%s] cross-check mismatch: %s", report.ds_id, m)
                report.record("cross-check", False,
                              f"{len(mismatches)} mismatch(es)")
            elif matched == 0:
                log.warning("[%s] cross-check: no overlapping observations "
                            "between the proto and om+json fetches (store "
                            "churned between requests) — nothing compared",
                            report.ds_id)
                report.record("cross-check", None, "no overlap")
            else:
                log.info("[%s] cross-check: %d observation(s) agree with "
                         "om+json", report.ds_id, matched)
                report.record("cross-check", True, f"{matched} matched")
        except Exception as exc:  # noqa: BLE001
            log_failure("cross-check", report.ds_id, exc,
                        "om+json reference fetch/compare failed")
            report.record("cross-check", False)

    # -- 6: MQTT --------------------------------------------------------------
    last_obs_time = max(
        (o.get("phenomenonTime", "") for o in proto_obs), default=None)
    run_mqtt_stage(ds, codec, report, last_obs_time)
    return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def discover_proto_datastreams(node: Node) -> list[tuple[Datastream, bool]]:
    """All discovered datastreams that serve ``application/swe+proto``.

    Returns ``(datastream, advertised)`` pairs. A datastream that lists
    the format in its ``formats`` array is taken at its word; one that
    doesn't is *probed* — a cheap schema GET — and still tested when the
    node actually serves the format. The probe exists because a node
    build was observed serving swe+proto on every endpoint while omitting
    it from `formats`; refusing to test in that state would hide the one
    finding that matters (the advertisement regression itself, reported
    by the ``advertised`` stage).
    """
    api = node.get_api_helper()
    out: list[tuple[Datastream, bool]] = []
    for system in node._systems:
        try:
            system.discover_datastreams()
        except Exception as exc:  # noqa: BLE001
            log.error("datastream discovery failed for system %s: %s",
                      getattr(system, "_resource_id", "?"), exc)
            log.debug("traceback:\n%s", traceback.format_exc())
            continue
        for ds in system.datastreams:
            formats = getattr(ds.get_resource(), "formats", None) or []
            if SWE_PROTO in formats:
                out.append((ds, True))
                continue
            try:
                probe = api.get_resource(
                    APIResourceTypes.DATASTREAM, ds.get_id(),
                    APIResourceTypes.SCHEMA, params={"obsFormat": SWE_PROTO})
                if probe.ok:
                    log.warning("[%s] not advertised but the schema endpoint "
                                "serves %s — testing it anyway",
                                ds.get_id(), SWE_PROTO)
                    out.append((ds, False))
                else:
                    log.debug("[%s] schema probe: HTTP %s — not a swe+proto "
                              "datastream", ds.get_id(), probe.status_code)
            except requests.exceptions.RequestException as exc:
                log.debug("[%s] schema probe failed: %s", ds.get_id(), exc)
    return out


def main() -> int:
    setup_logging()
    log.info("swe+proto node check — http://%s:%d (MQTT :%d)",
             HOST, HTTP_PORT, MQTT_PORT)
    log.info("log file: %s", LOG_PATH)

    osh = OSHConnect(name="swe-proto-check")
    try:
        node = Node(protocol="http", address=HOST, port=HTTP_PORT,
                    username=USER, password=PASS,
                    enable_mqtt=MQTT_SECS > 0, mqtt_port=MQTT_PORT)
    except OSError as exc:
        if MQTT_SECS <= 0:
            raise
        # A dead broker must not block the HTTP stages — fall back to an
        # HTTP-only node; run_mqtt_stage attributes the missing client.
        log.error("MQTT broker at %s:%d unreachable (%s) — continuing "
                  "HTTP-only (node side: MQTT service down or not started)",
                  HOST, MQTT_PORT, exc)
        log.debug("traceback:\n%s", traceback.format_exc())
        node = Node(protocol="http", address=HOST, port=HTTP_PORT,
                    username=USER, password=PASS, enable_mqtt=False)
    osh.add_node(node)

    try:
        osh.discover_systems()
    except requests.exceptions.RequestException as exc:
        log_failure("discovery", "node", exc, http_hint(exc))
        return 2
    log.info("discovered %d system(s)", len(node._systems))

    proto_streams = discover_proto_datastreams(node)
    if not proto_streams:
        log.error("no datastreams advertise or serve %s — node side (is the "
                  "consys-proto module installed and started?)", SWE_PROTO)
        return 2
    log.info("found %d swe+proto datastream(s): %s", len(proto_streams),
             [ds.get_id() for ds, _ in proto_streams])

    reports = [check_datastream(node, ds, advertised)
               for ds, advertised in proto_streams]

    # Clean MQTT shutdown so the broker sees a graceful close.
    client = node.get_mqtt_client()
    if client is not None:
        try:
            client.stop()
            client.disconnect()
        except Exception:  # noqa: BLE001
            pass

    log.info("")
    log.info("================ SUMMARY ================")
    any_failed = False
    for report in reports:
        log.info("%s (%s):", report.ds_id, report.name)
        for stage in STAGES:
            log.info("  %-13s %s", stage, report.stages.get(stage, "SKIP"))
        any_failed = any_failed or report.failed
    log.info("==========================================")
    log.info("result: %s — details in %s",
             "FAIL" if any_failed else "PASS", LOG_PATH)
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())

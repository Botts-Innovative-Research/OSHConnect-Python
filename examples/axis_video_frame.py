#!/usr/bin/env python
#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""End-to-end fidelity check for the SWE+binary codec against a live OSH node.

For each configured camera datastream, the script pulls H.264 frames as
``application/swe+binary``, decodes each record with `SWEBinaryCodec`,
**re-encodes** them, and pops a tkinter window comparing the H.264 frame
decoded from the OSH node's raw bytes against the frame decoded after a
full encode→decode roundtrip through ``SWEBinaryCodec`` +
``encode_swe_binary_blob``.

If the codec is faithful, the two panels on each row are pixel-identical
and the verdict label per camera reads "Byte-for-byte identical". The
GUI grows by one row per camera, so checking another camera is just one
more entry in `CAMERAS` (or one more ``label:ds_id`` in
``OSHC_AXIS_CAMERAS``).

Defaults
--------
* Node:        ``http://localhost:9191/sensorhub/api``
* Cameras:     ``Axis -> 040g``  and  ``Amcrest -> 025otg4indb0``
* Frames:      ``30`` per camera (enough to land a keyframe in practice)

Override with:

* ``OSHC_AXIS_PORT``     — server port (default ``9191``).
* ``OSHC_AXIS_FRAMES``   — frames per camera (default ``30``).
* ``OSHC_AXIS_CAMERAS``  — comma-separated ``Label:datastream_id`` pairs.
  Example: ``OSHC_AXIS_CAMERAS=Axis:040g,Amcrest:025otg4indb0``.

Run
---
    uv run python examples/axis_video_frame.py

The side-by-side GUI needs PyAV (for H.264 decode) and Pillow; install
them via the ``[av]`` extra::

    uv pip install -e ".[av]"

tkinter ships with most Python distributions, including the python.org
installer; on Homebrew or pyenv builds you may need to install the
``tcl-tk`` system package.
"""
from __future__ import annotations

import os
import struct
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

from oshconnect.schema_datamodels import SWEBinaryDatastreamRecordSchema
from oshconnect.swe_binary import SWEBinaryCodec, encode_swe_binary_blob


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PORT = os.environ.get("OSHC_AXIS_PORT", "9191")
N_FRAMES = int(os.environ.get("OSHC_AXIS_FRAMES", "30"))
BASE_URL = f"http://localhost:{PORT}/sensorhub/api"
OUT_DIR = Path("examples/_out")


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


# Default camera lineup: both video sources currently registered on the test
# node. Override via OSHC_AXIS_CAMERAS to add/remove cameras without code
# changes — useful when the demo is run against a different node.
CAMERAS = _parse_camera_env(
    os.environ.get("OSHC_AXIS_CAMERAS", "Axis:040g,Amcrest:025otg4indb0"))


# ---------------------------------------------------------------------------
# Per-camera result container
# ---------------------------------------------------------------------------


@dataclass
class CameraResult:
    """Everything one camera produced — used to drive the GUI grid."""
    label: str
    ds_id: str
    schema: SWEBinaryDatastreamRecordSchema
    codec: SWEBinaryCodec
    n_records: int
    frame_node: Optional["object"]   # numpy ndarray
    frame_codec: Optional["object"]  # numpy ndarray
    identical: bool
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def hex_window(label: str, raw: bytes, head: int = 16, tail: int = 8) -> None:
    """Print a labelled byte window — first `head` bytes, then last `tail`
    bytes — useful for visually comparing two payloads without scrolling
    through 20 kB of H.264.
    """
    if len(raw) <= head + tail:
        print(f"  {label} ({len(raw)} B): {raw.hex()}")
    else:
        print(f"  {label} ({len(raw)} B): {raw[:head].hex()}…{raw[-tail:].hex()}")


def fetch_schema(ds_id: str) -> SWEBinaryDatastreamRecordSchema:
    resp = requests.get(
        f"{BASE_URL}/datastreams/{ds_id}/schema",
        params={"obsFormat": "application/swe+binary"},
        timeout=5,
    )
    resp.raise_for_status()
    return SWEBinaryDatastreamRecordSchema.from_swebinary_dict(resp.json())


def fetch_observations(ds_id: str, limit: int) -> bytes:
    resp = requests.get(
        f"{BASE_URL}/datastreams/{ds_id}/observations",
        params={"f": "application/swe+binary", "limit": limit},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.content


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def compare_round_trip(label: str, codec: SWEBinaryCodec, raw: bytes) -> bool:
    """Decode → re-encode the first record; print + return byte-identity flag.

    Returns True if the codec produces byte-identical output for the first
    record. The full per-stream identity is computed later in
    `build_nal_streams`; this is the "fast confidence" check.
    """
    print(f"\n=== {label}: round-trip fidelity check (first record) ===")
    decoded, end = codec.decode_with_offset(raw, offset=0)
    print(f"Decoded first record (consumed {end} bytes):")
    print(f"  time = {decoded['time']:.6f}  (Unix epoch seconds)")
    print(f"  img  = {len(decoded['img'])} bytes of H.264 NAL data")
    print(f"  NAL start code: {decoded['img'][:4].hex()} (expect 00000001)")

    reencoded = encode_swe_binary_blob(decoded["img"], ts=decoded["time"])
    original_window = raw[:end]

    print("\nByte comparison:")
    hex_window("from node", original_window)
    hex_window("our codec", reencoded)
    if original_window == reencoded:
        print("✓ Byte-for-byte identical.")
        return True
    print("✗ Mismatch — divergence positions:")
    for i, (a, b) in enumerate(zip(original_window, reencoded)):
        if a != b:
            print(f"    offset {i}: node=0x{a:02x} ours=0x{b:02x}")
            if i > 16:
                print("    …(truncated)")
                break
    if len(original_window) != len(reencoded):
        print(f"    length differs: node={len(original_window)} ours={len(reencoded)}")
    return False


def save_nal_stream(codec: SWEBinaryCodec, raw: bytes, out_path: Path) -> int:
    """Walk every record in `raw`, concatenate its NAL payload to `out_path`.
    Returns the record count for sanity-printing."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    offset = 0
    total = 0
    with out_path.open("wb") as f:
        while offset < len(raw):
            rec, offset = codec.decode_with_offset(raw, offset=offset)
            f.write(rec["img"])
            total += len(rec["img"])
            count += 1
    print(f"Wrote {count} NAL units ({total} bytes) → {out_path}")
    return count


def build_nal_streams(codec: SWEBinaryCodec, raw: bytes) -> tuple[bytes, bytes, int]:
    """Walk every record twice — direct, and through the codec round-trip — to
    produce parallel NAL byte streams. Returns (node_stream, codec_stream, n)."""
    node_nals = bytearray()
    codec_nals = bytearray()
    offset = 0
    n_records = 0
    while offset < len(raw):
        rec, offset = codec.decode_with_offset(raw, offset=offset)
        node_nals += rec["img"]
        reframed = encode_swe_binary_blob(rec["img"], ts=rec["time"])
        rec2, _ = codec.decode_with_offset(reframed, offset=0)
        codec_nals += rec2["img"]
        n_records += 1
    return bytes(node_nals), bytes(codec_nals), n_records


def _decode_first_frame(nal_bytes: bytes):
    """Decode the first frame from an H.264 Annex B NAL stream.

    Returns an HxWx3 uint8 numpy array (RGB), or None if no frame could
    be decoded. PyAV handles Annex B start-code framing natively so we
    can feed the raw concatenated NAL bytes directly.
    """
    import io

    import av  # type: ignore

    try:
        with av.open(io.BytesIO(nal_bytes), "r", format="h264") as container:
            for frame in container.decode(video=0):
                return frame.to_ndarray(format="rgb24")
    except (OSError, ValueError) as exc:
        # PyAV raises OSError / ValueError for invalid streams; older
        # versions exposed `av.AVError` but it was removed in 11.x.
        print(f"  PyAV decode error: {exc}")
        return None
    return None


# ---------------------------------------------------------------------------
# Per-camera processing
# ---------------------------------------------------------------------------


def process_camera(label: str, ds_id: str, frames: int) -> CameraResult:
    """Run the full fidelity check for one camera datastream. Returns a
    `CameraResult` describing what was found, including decoded frames for
    the GUI step. Errors are captured on the result rather than raised so a
    failure for one camera doesn't kill the whole demo."""
    print(f"\n{'='*60}\n[{label}] datastream {ds_id}\n{'='*60}")

    schema = fetch_schema(ds_id)
    members = [m.ref for m in schema.record_encoding.members]
    print(f"✓ Fetched swe+binary schema; members: {members}")
    codec = SWEBinaryCodec(schema)

    raw = fetch_observations(ds_id, limit=frames)
    print(f"✓ Fetched {len(raw)} bytes ({frames} requested)")
    if len(raw) == 0:
        # OSH returns HTTP 200 with an empty body when the datastream has
        # no buffered observations — typically because the driver hasn't
        # connected to the source feed yet, or the source is offline. Treat
        # this as a non-fatal "not ready yet" and continue with the other
        # cameras.
        msg = ("no observations available yet (camera offline, RTP feed "
               "not connected, or no frames have been buffered)")
        print(f"[{label}] SKIP: {msg}")
        return CameraResult(label, ds_id, schema, codec, 0, None, None,
                            False, error=msg)

    try:
        compare_round_trip(label, codec, raw)
    except Exception as exc:  # noqa: BLE001
        return CameraResult(label, ds_id, schema, codec, 0, None, None,
                            False, error=f"round-trip failed: {exc}")

    h264_path = OUT_DIR / f"{label.lower()}_frames.h264"
    try:
        save_nal_stream(codec, raw, h264_path)
    except (struct.error, OSError) as exc:
        print(f"WARNING: error while saving NAL stream: {exc}")

    try:
        node_nals, codec_nals, n_records = build_nal_streams(codec, raw)
    except Exception as exc:  # noqa: BLE001
        return CameraResult(label, ds_id, schema, codec, 0, None, None,
                            False, error=f"stream build failed: {exc}")
    identical = node_nals == codec_nals
    print(f"\n[{label}] {n_records} records → {len(node_nals)} bytes per stream; "
          f"identical: {identical}")

    print(f"[{label}] decoding first frame of each stream with PyAV…")
    try:
        frame_node = _decode_first_frame(node_nals)
        frame_codec = _decode_first_frame(codec_nals)
    except ImportError:
        # GUI step requires PyAV; bare-bones runs without it still succeed.
        frame_node = frame_codec = None

    return CameraResult(label, ds_id, schema, codec, n_records,
                        frame_node, frame_codec, identical)


# ---------------------------------------------------------------------------
# GUI — one row per camera, two panels per row, plus a verdict label
# ---------------------------------------------------------------------------


def show_side_by_side_gui(results: list[CameraResult]) -> None:
    """Pop a tkinter window with one row per camera. Each row has two
    panels: frame decoded straight from the OSH wire, and frame decoded
    after a full encode→decode roundtrip through `SWEBinaryCodec`. A
    per-camera verdict label sits between rows.
    """
    try:
        import tkinter as tk

        import av  # noqa: F401
        from PIL import Image, ImageTk  # type: ignore
    except ImportError as exc:
        print("\n(GUI display needs PyAV + Pillow + tkinter:")
        print(f"   {exc}")
        print(" Install via:  uv pip install -e '.[av]')")
        return

    plottable = [r for r in results if r.frame_node is not None and r.frame_codec is not None]
    skipped = [r for r in results if r not in plottable]
    if not plottable:
        print("\n(No decodable frames across the configured cameras; "
              "skipping GUI.)")
        for r in skipped:
            print(f"   - {r.label}: {r.error or 'no frame decoded'}")
        return

    root = tk.Tk()
    root.title("OSH cameras — SWE+binary codec fidelity")
    container = tk.Frame(root, padx=12, pady=12)
    container.pack()

    # Header
    overall_ok = all(r.identical for r in plottable)
    skip_note = (f"  ({len(skipped)} skipped: "
                 f"{', '.join(r.label for r in skipped)})") if skipped else ""
    header_text = (f"{len(plottable)} camera(s) plotted · "
                   f"verdict: "
                   f"{'✓ all identical' if overall_ok else '✗ mismatch detected'}"
                   f"{skip_note}")
    header_color = "#1b8a3a" if overall_ok else "#b1331e"
    tk.Label(container, text=header_text,
             font=("Helvetica", 12, "bold"), fg=header_color).grid(
        row=0, column=0, columnspan=2, pady=(0, 8))

    tk.Label(container, text="From OSH node\n(direct H.264 decode)",
             font=("Helvetica", 11, "bold")).grid(row=1, column=0, padx=6)
    tk.Label(container, text="Through OSHConnect codec\n(decode → encode → decode)",
             font=("Helvetica", 11, "bold")).grid(row=1, column=1, padx=6)

    # Hold image references on the root so they're not garbage-collected
    # before tkinter renders them.
    root._photo_refs = []  # type: ignore[attr-defined]

    target_w = 520    # smaller than the single-camera version so the column fits two rows
    grid_row = 2
    for r in plottable:
        h, w = r.frame_node.shape[:2]  # type: ignore[union-attr]
        scale = min(1.0, target_w / w)
        new_size = (max(1, int(w * scale)), max(1, int(h * scale)))

        img_node = ImageTk.PhotoImage(
            Image.fromarray(r.frame_node).resize(new_size))
        img_codec = ImageTk.PhotoImage(
            Image.fromarray(r.frame_codec).resize(new_size))
        root._photo_refs.append((img_node, img_codec))  # type: ignore[attr-defined]

        tk.Label(container, image=img_node, borderwidth=2, relief="solid").grid(
            row=grid_row, column=0, padx=6, pady=(8, 2))
        tk.Label(container, image=img_codec, borderwidth=2, relief="solid").grid(
            row=grid_row, column=1, padx=6, pady=(8, 2))

        verdict = ("✓ byte-for-byte identical" if r.identical
                   else "✗ mismatch")
        color = "#1b8a3a" if r.identical else "#b1331e"
        meta = (f"{r.label} · ds {r.ds_id} · {r.n_records} records · "
                f"{w}×{h} → display {new_size[0]}×{new_size[1]}  ·  {verdict}")
        tk.Label(container, text=meta, font=("Helvetica", 10), fg=color).grid(
            row=grid_row + 1, column=0, columnspan=2, pady=(0, 8))

        grid_row += 2

    tk.Label(container, text="Close the window to exit.",
             font=("Helvetica", 9), fg="#666").grid(
        row=grid_row, column=0, columnspan=2, pady=(4, 0))

    root.mainloop()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    print(f"Base URL: {BASE_URL}")
    print(f"Cameras:  {', '.join(f'{lbl}/{ds}' for lbl, ds in CAMERAS)}")
    print(f"Frames:   {N_FRAMES} per camera")

    results: list[CameraResult] = []
    for label, ds_id in CAMERAS:
        try:
            results.append(process_camera(label, ds_id, N_FRAMES))
        except Exception as exc:  # noqa: BLE001
            # Don't let one bad camera kill the whole demo
            print(f"\n[{label}] ERROR: {exc}")
            results.append(CameraResult(
                label, ds_id, None, None, 0, None, None, False,  # type: ignore[arg-type]
                error=str(exc)))

    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    for r in results:
        if r.error:
            print(f"  {r.label} ({r.ds_id}): ERROR — {r.error}")
        else:
            print(f"  {r.label} ({r.ds_id}): "
                  f"{r.n_records} records, "
                  f"{'identical' if r.identical else 'MISMATCH'}")

    show_side_by_side_gui(results)
    print("\nDone.")
    return 0 if all(r.error is None and r.identical for r in results) else 2


if __name__ == "__main__":
    sys.exit(main())

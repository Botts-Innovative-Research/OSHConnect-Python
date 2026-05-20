#!/usr/bin/env python
#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""End-to-end fidelity check for the SWE+binary codec against a live OSH node.

Hits an Axis-camera-backed OSH datastream, pulls H.264 frames as
``application/swe+binary``, decodes each record with `SWEBinaryCodec`,
**re-encodes** them, and pops a side-by-side tkinter window comparing:

* the H.264 frame decoded from the *raw* bytes the OSH node sent, and
* the H.264 frame decoded after a full encode→decode roundtrip through
  ``SWEBinaryCodec`` + ``encode_swe_binary_blob``.

If the codec is faithful, the two panels are pixel-identical and the
verdict label reads "Byte-for-byte identical". Any divergence shows up
visually and in the printed byte-comparison.

Defaults
--------
* Node:        ``http://localhost:9191/sensorhub/api`` (the Axis test node)
* Datastream:  ``040g`` (the ``video1`` output)
* Frames:      ``30`` (enough to land at least one keyframe in practice)

Override with the env vars ``OSHC_AXIS_PORT``, ``OSHC_AXIS_DS``, and
``OSHC_AXIS_FRAMES`` respectively.

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
from pathlib import Path

import requests

from oshconnect.schema_datamodels import SWEBinaryDatastreamRecordSchema
from oshconnect.swe_binary import SWEBinaryCodec, encode_swe_binary_blob


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PORT = os.environ.get("OSHC_AXIS_PORT", "9191")
DS_ID = os.environ.get("OSHC_AXIS_DS", "040g")
N_FRAMES = int(os.environ.get("OSHC_AXIS_FRAMES", "30"))
BASE_URL = f"http://localhost:{PORT}/sensorhub/api"
OUT_DIR = Path("examples/_out")


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


def fetch_schema() -> SWEBinaryDatastreamRecordSchema:
    resp = requests.get(
        f"{BASE_URL}/datastreams/{DS_ID}/schema",
        params={"obsFormat": "application/swe+binary"},
        timeout=5,
    )
    resp.raise_for_status()
    return SWEBinaryDatastreamRecordSchema.from_swebinary_dict(resp.json())


def fetch_observations(limit: int) -> bytes:
    resp = requests.get(
        f"{BASE_URL}/datastreams/{DS_ID}/observations",
        params={"f": "application/swe+binary", "limit": limit},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.content


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def compare_round_trip(codec: SWEBinaryCodec, raw: bytes) -> bytes:
    """Decode → re-encode the first record; print + assert byte-identity.

    Returns the H.264 NAL bytes for the first decoded record so the caller
    can save them.
    """
    print("\n=== Round-trip fidelity check (first record) ===")
    decoded, end = codec.decode_with_offset(raw, offset=0)
    print(f"Decoded first record (consumed {end} bytes):")
    print(f"  time = {decoded['time']:.6f}  (Unix epoch seconds)")
    print(f"  img  = {len(decoded['img'])} bytes of H.264 NAL data")
    print(f"  NAL start code: {decoded['img'][:4].hex()} (expect 00000001)")

    # Re-encode with our codec
    reencoded = encode_swe_binary_blob(decoded["img"], ts=decoded["time"])
    original_window = raw[:end]

    print("\nByte comparison:")
    hex_window("from node", original_window)
    hex_window("our codec", reencoded)
    if original_window == reencoded:
        print("\n✓ Byte-for-byte identical.")
    else:
        print("\n✗ Mismatch — divergence positions:")
        for i, (a, b) in enumerate(zip(original_window, reencoded)):
            if a != b:
                print(f"    offset {i}: node=0x{a:02x} ours=0x{b:02x}")
                if i > 16:
                    print("    …(truncated)")
                    break
        if len(original_window) != len(reencoded):
            print(f"    length differs: node={len(original_window)} ours={len(reencoded)}")

    return decoded["img"]


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
    print(f"\nWrote {count} NAL units ({total} bytes) → {out_path}")
    return count


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


def show_side_by_side_gui(codec: SWEBinaryCodec, raw: bytes) -> None:
    """Show side-by-side: frame as decoded from the OSH node's raw wire bytes
    vs. frame as decoded after a full encode→decode round-trip through our codec.

    Walks every record in `raw` to build two parallel NAL streams (one
    direct, one through the codec). Decodes the first frame of each and
    presents them in a tkinter window with a match/mismatch verdict.
    """
    try:
        import tkinter as tk

        import av  # noqa: F401  (PyAV needed for _decode_first_frame)
        from PIL import Image, ImageTk  # type: ignore
    except ImportError as exc:
        print("\n(GUI display needs PyAV + Pillow + tkinter:")
        print(f"   {exc}")
        print(" Install via:  uv pip install -e '.[av]')")
        return

    print("\n=== Building parallel NAL streams (node vs. codec) ===")
    node_nals = bytearray()
    codec_nals = bytearray()
    offset = 0
    n_records = 0
    while offset < len(raw):
        rec, offset = codec.decode_with_offset(raw, offset=offset)
        node_nals += rec["img"]
        # Round-trip through our codec, then re-decode to extract the NAL.
        reframed = encode_swe_binary_blob(rec["img"], ts=rec["time"])
        rec2, _ = codec.decode_with_offset(reframed, offset=0)
        codec_nals += rec2["img"]
        n_records += 1
    print(f"  {n_records} records → {len(node_nals)} bytes per stream")
    identical = bytes(node_nals) == bytes(codec_nals)
    print(f"  NAL streams identical: {identical}")

    print("Decoding first frame of each stream with PyAV…")
    frame_node = _decode_first_frame(bytes(node_nals))
    frame_codec = _decode_first_frame(bytes(codec_nals))
    if frame_node is None or frame_codec is None:
        print("  could not decode at least one stream; skipping GUI.")
        return

    h, w = frame_node.shape[:2]
    # Resize so the side-by-side fits a typical laptop screen (~1400 px wide).
    target_w = 600
    scale = min(1.0, target_w / w)
    new_size = (max(1, int(w * scale)), max(1, int(h * scale)))

    root = tk.Tk()
    root.title("OSH camera — SWE+binary codec fidelity")

    container = tk.Frame(root, padx=12, pady=12)
    container.pack()

    header_text = (
        f"Datastream {DS_ID}   ·   {n_records} records   ·   "
        f"{w}×{h} → display {new_size[0]}×{new_size[1]}"
    )
    tk.Label(container, text=header_text, font=("Helvetica", 11)).grid(
        row=0, column=0, columnspan=2, pady=(0, 8))

    tk.Label(container, text="From OSH node\n(direct H.264 decode)",
             font=("Helvetica", 12, "bold")).grid(row=1, column=0, padx=6)
    tk.Label(container, text="Through OSHConnect codec\n(decode → encode → decode)",
             font=("Helvetica", 12, "bold")).grid(row=1, column=1, padx=6)

    # Keep refs alive on the root or they're garbage-collected before render.
    root._img_node = ImageTk.PhotoImage(Image.fromarray(frame_node).resize(new_size))
    root._img_codec = ImageTk.PhotoImage(Image.fromarray(frame_codec).resize(new_size))
    tk.Label(container, image=root._img_node, borderwidth=2, relief="solid").grid(
        row=2, column=0, padx=6, pady=4)
    tk.Label(container, image=root._img_codec, borderwidth=2, relief="solid").grid(
        row=2, column=1, padx=6, pady=4)

    verdict = "✓ Byte-for-byte identical" if identical else "✗ Mismatch"
    color = "#1b8a3a" if identical else "#b1331e"
    tk.Label(container, text=f"NAL stream verdict: {verdict}",
             font=("Helvetica", 12, "bold"), fg=color).grid(
        row=3, column=0, columnspan=2, pady=(10, 0))

    tk.Label(container,
             text="Close the window to exit.",
             font=("Helvetica", 9), fg="#666").grid(
        row=4, column=0, columnspan=2, pady=(4, 0))

    root.mainloop()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    print(f"Hitting {BASE_URL}/datastreams/{DS_ID}")

    try:
        schema = fetch_schema()
    except Exception as exc:
        print(f"ERROR: could not fetch schema: {exc}")
        return 1
    print("✓ Fetched swe+binary schema")
    members = [m.ref for m in schema.record_encoding.members]
    print(f"  members: {members}")

    codec = SWEBinaryCodec(schema)

    try:
        raw = fetch_observations(limit=N_FRAMES)
    except Exception as exc:
        print(f"ERROR: could not fetch observations: {exc}")
        return 1
    print(f"✓ Fetched {len(raw)} bytes ({N_FRAMES} requested)")

    # Round-trip the first record
    try:
        compare_round_trip(codec, raw)
    except Exception as exc:
        print(f"ERROR: round-trip failed: {exc}")
        return 1

    # Save the full NAL stream
    h264_path = OUT_DIR / "axis_frames.h264"
    try:
        save_nal_stream(codec, raw, h264_path)
    except struct.error as exc:
        print(f"WARNING: could not walk all records ({exc}) — partial file written")
    except Exception as exc:
        print(f"WARNING: error while saving NAL stream: {exc}")

    # Pop the side-by-side comparison GUI. Blocks until the user closes
    # the window; skipped automatically when PyAV/Pillow/tkinter aren't
    # available.
    show_side_by_side_gui(codec, raw)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
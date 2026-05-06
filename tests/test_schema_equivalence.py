#  =============================================================================
#  Copyright (c) 2026 Botts Innovative Research Inc.
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
"""
Verify that OSHConnect's datamodels can faithfully represent the datastream schema
that an OSH server publishes for the FakeWeatherDriver, in both observation
formats served:

  - application/om+json   (CS API Part 2 §16.1.4 shape: obsFormat + resultSchema)
  - application/swe+json  (CS API Part 2 §16.2.3 shape: obsFormat + recordSchema
                          [+ encoding])

Strategy: round-trip the server-supplied schema JSON through the matching
pydantic model (parse -> re-serialize) and assert structural equivalence. If
our datamodels can losslessly express what the Node has, then a schema
*generated* from those same datamodels will match the Node.

Each parametrized case prefers a live node at localhost:8282 (FakeWeatherDriver
running). If the node is unreachable or no weather system is registered, it
falls back to the saved fixture at tests/fixtures/fake_weather_schema_<fmt>.json.
If neither is available, the case is skipped.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple

import pytest
import requests

from src.oshconnect.schema_datamodels import (
    OMJSONDatastreamRecordSchema,
    SWEDatastreamRecordSchema,
)

NODE_URL = "http://localhost:8282/sensorhub/api"
NODE_AUTH = ("admin", "admin")
LIVE_TIMEOUT = 2.0
FIXTURES_DIR = Path(__file__).parent / "fixtures"


class FormatCase(NamedTuple):
    obs_format: str
    model: type
    fixture_path: Path


CASES = [
    FormatCase(
        obs_format="application/om+json",
        model=OMJSONDatastreamRecordSchema,
        fixture_path=FIXTURES_DIR / "fake_weather_schema_omjson.json",
    ),
    FormatCase(
        obs_format="application/swe+json",
        model=SWEDatastreamRecordSchema,
        fixture_path=FIXTURES_DIR / "fake_weather_schema_swejson.json",
    ),
]


def _find_weather_system(systems: list[dict]) -> dict | None:
    """Pick a system whose name/description/uid mentions 'weather'."""
    for sys_ in systems:
        props = sys_.get("properties", {}) or {}
        haystack = " ".join(
            str(x) for x in (
                sys_.get("id", ""),
                props.get("name", ""),
                props.get("description", ""),
                props.get("uid", ""),
            )
        ).lower()
        if "weather" in haystack:
            return sys_
    return None


def _try_live_schema(obs_format: str) -> tuple[str, dict] | None:
    """Probe the node at localhost:8282 for a FakeWeather datastream and return
    (source_label, schema_json) for the requested obs_format. Returns None on
    any failure."""
    try:
        sys_resp = requests.get(f"{NODE_URL}/systems?f=json", auth=NODE_AUTH, timeout=LIVE_TIMEOUT)
    except (requests.ConnectionError, requests.Timeout):
        return None
    if not sys_resp.ok:
        return None

    weather = _find_weather_system(sys_resp.json().get("items", []))
    if not weather:
        return None

    sys_id = weather.get("id")
    if not sys_id:
        return None

    ds_resp = requests.get(
        f"{NODE_URL}/systems/{sys_id}/datastreams?f=json",
        auth=NODE_AUTH, timeout=LIVE_TIMEOUT,
    )
    if not ds_resp.ok:
        return None
    datastreams = ds_resp.json().get("items", [])
    if not datastreams:
        return None

    ds_id = datastreams[0].get("id")
    schema_resp = requests.get(
        f"{NODE_URL}/datastreams/{ds_id}/schema",
        params={"obsFormat": obs_format},
        auth=NODE_AUTH, timeout=LIVE_TIMEOUT,
    )
    if not schema_resp.ok:
        return None

    return (
        f"live node 8282 ({obs_format}, system={sys_id}, datastream={ds_id})",
        schema_resp.json(),
    )


def _try_fixture_schema(path: Path) -> tuple[str, dict] | None:
    """Load the saved fixture if it exists and is non-empty."""
    if not path.exists():
        return None
    text = path.read_text().strip()
    if not text or text == "{}":
        return None
    data = json.loads(text)
    if not data:
        return None
    return f"fixture {path.name}", data


@pytest.mark.parametrize(
    "case",
    CASES,
    ids=lambda c: c.obs_format,
)
def test_fake_weather_schema_round_trips_through_datamodels(case: FormatCase):
    source = _try_live_schema(case.obs_format) or _try_fixture_schema(case.fixture_path)
    if source is None:
        pytest.skip(
            f"No live FakeWeather node at {NODE_URL} for {case.obs_format} and no "
            f"usable fixture at {case.fixture_path}. To enable: start the "
            f"FakeWeatherDriver on the node, or paste a schema JSON into the fixture."
        )
    label, server_schema = source

    parsed = case.model.model_validate(server_schema)
    round_tripped = parsed.model_dump(
        mode='json', by_alias=True, exclude_none=True, exclude_unset=True,
    )

    assert server_schema == round_tripped, (
        f"Schema round-trip mismatch (source: {label}, model: {case.model.__name__}).\n"
        f"server:\n{json.dumps(server_schema, indent=2, sort_keys=True)}\n\n"
        f"datamodel re-serialization:\n{json.dumps(round_tripped, indent=2, sort_keys=True)}"
    )
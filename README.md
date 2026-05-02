# OSHConnect-Python

Library for communicating with Opensensorhub that provides options for saving configurations, getting visualization 
recommendations for data, retrieving data in real time, archival streams, and batch modes, and more.

API Documentation available [here](https://botts-innovative-research.github.io/OSHConnect-Python/)

Links:
 * [Architecture Doc](https://docs.google.com/document/d/1pIaeQw0ocU6ApNgqTVRZuSwjJAbhCcmweMq6RiVYEic/edit?usp=sharing)
 * [UML Diagram](https://drive.google.com/file/d/1FVrnYiuAR8ykqfOUa1NuoMyZ1abXzMPw/view?usp=drive_link)

## Running Tests

```bash
uv sync                                # install dev deps (incl. pytest, pytest-cov)
uv run pytest                          # full suite (skips network-marked tests if you add `-m "not network"`)
uv run pytest tests/test_swe_components.py -v   # one file, verbose
uv run pytest -k name_token            # one keyword
```

Tests that need a live OSH server (e.g. `localhost:8282` running
FakeWeatherDriver) are tagged `@pytest.mark.network`. CI skips them; locally
you can include or exclude them:

```bash
uv run pytest -m "not network"         # what CI runs
uv run pytest -m network               # only the live-server tests
```

## Test Coverage

Coverage is opt-in via [`pytest-cov`](https://pytest-cov.readthedocs.io/). The
default `pytest` run is fast; add `--cov` when you want a report.

```bash
uv run pytest --cov                    # terminal summary + missing lines
uv run pytest --cov --cov-report=html  # HTML report at htmlcov/index.html
uv run pytest --cov --cov-report=xml   # coverage.xml (CI / Codecov-ready)
```

Configuration lives in `pyproject.toml` under `[tool.coverage.*]` — branch
coverage is on, source is scoped to `src/oshconnect`, and obvious dead lines
(`if TYPE_CHECKING:`, `raise NotImplementedError`, etc.) are excluded.

CI (`.github/workflows/tests.yaml`) runs the suite with `--cov` on every push
across Python 3.12 / 3.13 / 3.14 and uploads `coverage.xml` as a workflow
artifact (downloadable from the run page).

## Documentation Coverage

[`interrogate`](https://interrogate.readthedocs.io/) reports what fraction of
public modules / classes / functions / methods carry a docstring (presence
only, it doesn't check style). It's purely informational right now; there's
no CI gate. Configuration lives in `pyproject.toml` under `[tool.interrogate]`
(`__init__`, dunder, private, and property/setter members are skipped).

```bash
uv run interrogate src/oshconnect              # one-line summary
uv run interrogate -v src/oshconnect           # per-file table
uv run interrogate -vv src/oshconnect          # per-symbol (shows which symbols are missing)
```

Once we agree on a baseline, raise `[tool.interrogate].fail-under` from `0` so
new code without docstrings starts failing locally and in CI.

## Generating the Docs

The documentation is built with [MkDocs](https://www.mkdocs.org/) using the
Material theme, [mkdocstrings](https://mkdocstrings.github.io/) for
auto-generated API reference from the source, and
[mermaid](https://mermaid.js.org/) for architecture diagrams. Markdown sources
live under `docs/markdown/`.

Install dev dependencies (including MkDocs and plugins):

```bash
uv sync
```

Build the HTML docs:

```bash
uv run mkdocs build
```

The output will be in `docs/build/html/`. Open `docs/build/html/index.html` in
a browser to view locally.

For a live-reloading preview while editing:

```bash
uv run mkdocs serve
```

Then visit http://127.0.0.1:8000.

To match what CI publishes (warnings become errors — useful when you've
touched docstrings):

```bash
uv run mkdocs build --strict
```

CI builds the site on every push and deploys `main` to GitHub Pages via
`.github/workflows/docs_pages.yaml`.

The legacy Sphinx setup under `docs/source/` is kept temporarily for
reference and builds to a separate output directory:

```bash
uv run sphinx-build -b html docs/source docs/build/sphinx
```
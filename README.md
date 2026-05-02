# OSHConnect-Python

Library for communicating with Opensensorhub that provides options for saving configurations, getting visualization 
recommendations for data, retrieving data in real time, archival streams, and batch modes, and more.

API Documentation available [here](https://botts-innovative-research.github.io/OSHConnect-Python/)

Links:
 * [Architecture Doc](https://docs.google.com/document/d/1pIaeQw0ocU6ApNgqTVRZuSwjJAbhCcmweMq6RiVYEic/edit?usp=sharing)
 * [UML Diagram](https://drive.google.com/file/d/1FVrnYiuAR8ykqfOUa1NuoMyZ1abXzMPw/view?usp=drive_link)

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
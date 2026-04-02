# OSHConnect-Python

Library for communicating with Opensensorhub that provides options for saving configurations, getting visualization 
recommendations for data, retrieving data in real time, archival streams, and batch modes, and more.

API Documentation available [here](https://botts-innovative-research.github.io/OSHConnect-Python/)

Links:
 * [Architecture Doc](https://docs.google.com/document/d/1pIaeQw0ocU6ApNgqTVRZuSwjJAbhCcmweMq6RiVYEic/edit?usp=sharing)
 * [UML Diagram](https://drive.google.com/file/d/1FVrnYiuAR8ykqfOUa1NuoMyZ1abXzMPw/view?usp=drive_link)

## Generating the Docs

The documentation is built with [Sphinx](https://www.sphinx-doc.org/). Dev dependencies (including Sphinx) are installed automatically with:

```bash
uv sync
```

Then build the HTML docs:

```bash
uv run sphinx-build -b html docs/source docs/build/html
```

The output will be in `docs/build/html/`. Open `docs/build/html/index.html` in a browser to view locally.

To do a clean rebuild:

```bash
uv run sphinx-build -E -b html docs/source docs/build/html
```

The `-E` flag forces Sphinx to re-read all source files rather than using cached data.
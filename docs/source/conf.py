# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys
import traceback

# Make the package importable for autodoc.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))


def process_exception(app, what, name, obj, options, lines):
    traceback.print_exc()


def setup(app):
    app.connect('autodoc-process-docstring', process_exception)


# -- Project information -----------------------------------------------------

project = 'OSHConnect-Python'
copyright = '2025-2026, Botts Innovative Research, Inc.'
author = 'Ian Patterson'
release = '0.5.1'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',         # API ref from docstrings
    'sphinx.ext.autosummary',     # autodoc summaries
    'sphinx.ext.napoleon',        # Google / Sphinx docstring styles
    'sphinx.ext.viewcode',        # link to source on each symbol
    'sphinx.ext.intersphinx',     # cross-link to Python stdlib / pydantic
    'sphinx.ext.doctest',
    'sphinx.ext.duration',
    'myst_parser',                # Markdown support (so we can keep .md sources)
    'sphinxcontrib.mermaid',      # mermaid diagrams from architecture.md
    'sphinx_copybutton',          # copy-to-clipboard on code blocks
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

templates_path = ['_templates']
exclude_patterns = []

# -- Autodoc / Napoleon ------------------------------------------------------

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
    'member-order': 'bysource',
    # `handle_aliases` is a pydantic before-validator that autodoc can't
    # introspect (it's wrapped in a PydanticDescriptorProxy). Hide it.
    'exclude-members': 'handle_aliases,model_config,model_fields,model_computed_fields',
}
autodoc_typehints = 'description'  # render type hints into the param table
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

# -- MyST (Markdown) ---------------------------------------------------------

myst_enable_extensions = [
    'colon_fence',     # ::: admonition syntax
    'deflist',
    'html_admonition',
    'html_image',
    'tasklist',
]
myst_heading_anchors = 3

# Route ```mermaid fenced blocks through sphinxcontrib-mermaid so the existing
# `architecture.md` diagrams render visually instead of as raw code.
myst_fence_as_directive = ['mermaid']

# Don't fail on the intentional re-exports between `oshconnect.eventbus`
# and `oshconnect.events.core` (AtomicEventTypes is exposed at both names).
suppress_warnings = [
    'duplicate_object_description',
]

# -- Intersphinx -------------------------------------------------------------

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pydantic': ('https://docs.pydantic.dev/latest', None),
}

# -- Mermaid -----------------------------------------------------------------

mermaid_version = 'latest'

# -- HTML output (Furo) ------------------------------------------------------

html_theme = 'furo'
# html_static_path is omitted — we don't ship custom CSS/JS yet. Add it
# back as ['_static'] (and create the directory) when there's something
# to put in there.
html_title = 'OSHConnect-Python'
html_theme_options = {
    'sidebar_hide_name': False,
    'navigation_with_keys': True,
    'source_repository': 'https://github.com/Botts-Innovative-Research/OSHConnect-Python',
    'source_branch': 'main',
    'source_directory': 'docs/source/',
    'top_of_page_buttons': ['view', 'edit'],
}

# Kubeflow Spark Operator Documentation

This directory contains the source for the Kubeflow Spark Operator documentation
website, built with [Sphinx](https://www.sphinx-doc.org/), the
[Furo](https://github.com/pradyunsg/furo) theme, and
[MyST Markdown](https://myst-parser.readthedocs.io/). It is designed to be hosted
on [Read the Docs](https://readthedocs.org/).

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (manages Python and all dependencies)

uv handles the Python toolchain and the documentation dependencies declared in
[`pyproject.toml`](./pyproject.toml) (the `docs` dependency group, pinned in
[`uv.lock`](./uv.lock)). There is no need to create a virtual environment by hand.

## Building locally

From the repository root:

```bash
make docs            # build static HTML into docs/website/_build/html
make docs-test       # strict build (warnings treated as errors)
make docs-serve      # build and serve with live reload at http://localhost:8000
make docs-linkcheck  # validate external links
make docs-clean      # remove build artifacts
```

Or run uv directly from this directory:

```bash
cd docs/website
uv run --group docs sphinx-build -b html . _build/html
uv run --group docs sphinx-autobuild . _build/html --port 8000   # live reload
```

The generated site is written to `docs/website/_build/html`. Open
`docs/website/_build/html/index.html` in a browser, or use `make docs-serve` for
live reload.

## Layout

```
docs/website/
├── pyproject.toml          # Python deps (uv "docs" group) + uv.lock
├── conf.py                 # Sphinx configuration
├── index.rst               # Landing page (custom HTML hero)
├── Makefile                # html / test / serve / linkcheck / clean targets
├── _ext/kubeflow_topnav/   # Sphinx extension: top navigation bar
├── _static/
│   ├── css/custom.css      # Kubeflow theme + landing page styles (light & dark)
│   ├── img/                # logos, architecture diagram, CNCF mark
│   └── js/                 # external-links, sidebar-toggle, landing-page, etc.
├── overview/               # Overview
├── getting-started/        # Installation & first SparkApplication
├── user-guide/             # In-depth user guides
├── performance/            # Benchmarking
├── reference/              # API reference
└── contributor-guide/      # Developer guide
```

## Read the Docs

The build is configured by [`.readthedocs.yaml`](../../.readthedocs.yaml) at the
repository root, which runs `uv sync` and `uv run sphinx-build` against
`docs/website`.

Documentation content is distributed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

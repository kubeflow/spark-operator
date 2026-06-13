# Kubeflow Spark Operator Documentation

This directory contains the source for the Kubeflow Spark Operator documentation
website, built with [Sphinx](https://www.sphinx-doc.org/), the
[Furo](https://github.com/pradyunsg/furo) theme, and
[MyST Markdown](https://myst-parser.readthedocs.io/). It is designed to be hosted
on [Read the Docs](https://readthedocs.org/).

## Prerequisites

- Python 3.12+
- The dependencies listed in [`requirements.txt`](./requirements.txt)

## Building locally

Create a virtual environment and install the dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r docs/requirements.txt
```

Then, from the repository root, build the HTML site:

```bash
make docs          # build static HTML into docs/_build/html
make docs-serve    # build and serve with live reload at http://localhost:8000
make docs-linkcheck  # validate external links
make docs-clean    # remove build artifacts
```

Or run the targets directly from this directory:

```bash
cd docs
make html
make serve
```

The generated site is written to `docs/_build/html`. Open
`docs/_build/html/index.html` in a browser, or use `make docs-serve` for live reload.

## Layout

```
docs/
├── conf.py                 # Sphinx configuration
├── index.rst               # Landing page (custom HTML hero)
├── requirements.txt        # Python build dependencies
├── Makefile                # html / serve / linkcheck / clean targets
├── _ext/kubeflow_topnav/   # Sphinx extension: top navigation bar
├── _static/
│   ├── css/custom.css      # Kubeflow theme + landing page styles (light & dark)
│   └── js/                 # external-links, sidebar-toggle, landing-page
├── overview/               # Overview
├── getting-started/        # Installation & first SparkApplication
├── user-guide/             # In-depth user guides
├── performance/            # Benchmarking
├── reference/              # API reference
└── contributor-guide/      # Developer guide
```

## Read the Docs

The build is configured by [`.readthedocs.yaml`](../.readthedocs.yaml) at the
repository root, which points at `docs/conf.py`.

Documentation content is distributed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

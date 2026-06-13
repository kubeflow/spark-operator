# Copyright The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configuration file for the Sphinx documentation builder.
# Kubeflow Spark Operator Documentation System

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "_ext"))

# -- Project information -----------------------------------------------------
project = "Kubeflow Spark Operator"
copyright = "2026, Kubeflow Authors"
author = "Kubeflow Authors"

# The version is set from environment variable or defaults to "latest"
# ReadTheDocs sets READTHEDOCS_VERSION automatically
version = os.getenv("READTHEDOCS_VERSION", "latest")
release = version

# -- General configuration ---------------------------------------------------
extensions = [
    "myst_parser",  # Markdown support via MyST
    "sphinxcontrib.mermaid",  # Mermaid diagram rendering
    "sphinx_copybutton",  # Copy button on code blocks
    "sphinx_design",  # Grid layouts and card components
    "sphinxext.opengraph",  # Open Graph / social preview cards
    "kubeflow_topnav",  # Top navigation bar (see docs/_ext/kubeflow_topnav/)
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "*.egg-info",
    "__pycache__",
    "README.md",
    # Pre-existing standalone docs not part of the Sphinx navigation
    "api-docs.md",
    "kustomize-installation.md",
    "release.md",
]

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_title = "Kubeflow Spark Operator"
html_static_path = ["_static"]
html_favicon = "_static/img/kubeflow-spark-operator-icon.png"
html_css_files = ["css/custom.css"]
html_js_files = [
    "js/external-links.js",
    "js/sidebar-toggle.js",
    "js/landing-page.js",
    "js/theme-toggle.js",
]

# Furo theme options
html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#4299e1",
        "color-brand-content": "#3182ce",
        "color-foreground-primary": "#1a202c",
        "color-foreground-secondary": "#2d3748",
        "color-foreground-muted": "#4a5568",
        "color-foreground-border": "#e2e8f0",
    },
    "dark_css_variables": {
        "color-brand-primary": "#63b3ed",
        "color-brand-content": "#63b3ed",
        "color-foreground-primary": "#e2e8f0",
        "color-foreground-secondary": "#cbd5e0",
        "color-foreground-muted": "#a0aec0",
        "color-foreground-border": "#4a5568",
    },
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
    "top_of_page_buttons": ["view", "edit"],
    "source_repository": "https://github.com/kubeflow/spark-operator",
    "source_branch": "master",
    "source_directory": "docs/",
}

# ReadTheDocs version switcher integration
# These variables are set by ReadTheDocs at build time
html_context = {
    "display_github": True,
    "github_user": "kubeflow",
    "github_repo": "spark-operator",
    "github_version": "master",
    "conf_py_path": "/docs/",
}

# Canonical site URL — used for canonical tags and Open Graph absolute URLs.
# ReadTheDocs sets READTHEDOCS_CANONICAL_URL automatically at build time.
html_baseurl = os.getenv(
    "READTHEDOCS_CANONICAL_URL", "https://kubeflow.github.io/spark-operator/"
)

# -- Open Graph / social preview cards ---------------------------------------
ogp_site_url = html_baseurl
ogp_site_name = "Kubeflow Spark Operator"
ogp_image = "_static/img/kubeflow-spark-operator-logo.png"
ogp_description_length = 200
ogp_enable_meta_description = True
ogp_custom_meta_tags = [
    '<meta name="twitter:card" content="summary_large_image">',
]

# -- MyST Parser configuration -----------------------------------------------
myst_enable_extensions = [
    "colon_fence",  # ::: fence syntax for directives
    "deflist",  # Definition lists
    "fieldlist",  # Field lists
    "substitution",  # Variable substitution
    "tasklist",  # Task lists [ ] [x]
]
myst_links_external_new_tab = True
myst_heading_anchors = 4

# -- Mermaid configuration ---------------------------------------------------
mermaid_version = "10.9.1"
mermaid_d3_zoom = False
mermaid_init_js = """
mermaid.initialize({
    startOnLoad: true,
    theme: 'default',
    flowchart: {
        htmlLabels: false
    }
});
"""

# -- Link checking configuration ---------------------------------------------
linkcheck_ignore = [
    r"http://localhost:\d+/",  # Ignore localhost links
    r"https://github\.com/.*/pulls/.*",  # GitHub PR links may be private
]

# -- Copy button configuration -----------------------------------------------
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True
copybutton_line_continuation_character = "\\"

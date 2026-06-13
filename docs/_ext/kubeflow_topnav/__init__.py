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

"""Inject the Kubeflow Spark Operator top navigation bar via a Sphinx extension.

The ribbon logo is embedded as a base64 data URI so it renders correctly on
every page regardless of nesting depth or hosting base path (the announcement
bar is static HTML and is not run through Sphinx's path resolution).
"""

import base64
from pathlib import Path

# Logo shown before the brand text in the top ribbon.
LOGO_PATH = (
    Path(__file__).parents[2] / "_static" / "img" / "kubeflow-spark-operator-icon.png"
)


def _logo_data_uri():
    data = LOGO_PATH.read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def setup(app):
    topnav_html = (Path(__file__).parent / "topnav.html").read_text(encoding="utf-8")
    topnav_html = topnav_html.replace("__LOGO_DATA_URI__", _logo_data_uri())
    app.config.html_theme_options["announcement"] = topnav_html
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }

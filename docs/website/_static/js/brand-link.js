/**
 * Copyright 2026 The Kubeflow authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Point the ribbon brand/logo at the documentation root *relative to the current
// page*, so it works under any hosting base path — Read the Docs (/en/latest/),
// a GitHub Pages project subpath (/spark-operator/), or a local build — without
// hardcoding an absolute URL. Sphinx (>= 7.2) exposes the per-page root via the
// `data-content_root` attribute on the <html> element; older versions expose it
// via DOCUMENTATION_OPTIONS.URL_ROOT.
document.addEventListener("DOMContentLoaded", function () {
  var brand = document.querySelector(".top-nav-brand");
  if (!brand) return;

  var root = null;
  var el = document.querySelector("[data-content_root]");
  if (el) {
    root = el.getAttribute("data-content_root");
  } else if (
    typeof DOCUMENTATION_OPTIONS !== "undefined" &&
    DOCUMENTATION_OPTIONS.URL_ROOT != null
  ) {
    root = DOCUMENTATION_OPTIONS.URL_ROOT;
  }
  if (!root) root = "./";
  if (root.charAt(root.length - 1) !== "/") root += "/";

  brand.setAttribute("href", root + "index.html");
  // It is internal navigation — ensure it stays in the same tab.
  brand.removeAttribute("target");
  brand.removeAttribute("rel");
});

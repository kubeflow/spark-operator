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

// Wire the ribbon light/dark toggle into Furo's theme system.
// Furo stores the chosen theme in localStorage under "theme" and reflects it
// on document.body.dataset.theme ("auto" | "light" | "dark").
document.addEventListener("DOMContentLoaded", function () {
  var btn = document.querySelector(".top-nav-theme-toggle");
  if (!btn) return;

  function effectiveMode() {
    var t = document.body.dataset.theme || "auto";
    if (t === "auto") {
      return window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light";
    }
    return t;
  }

  function applyTheme(mode) {
    document.body.dataset.theme = mode;
    try {
      localStorage.setItem("theme", mode);
    } catch (e) {
      /* ignore storage errors */
    }
  }

  btn.addEventListener("click", function () {
    applyTheme(effectiveMode() === "dark" ? "light" : "dark");
  });
});

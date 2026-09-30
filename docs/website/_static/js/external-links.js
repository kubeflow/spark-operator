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

document.addEventListener("DOMContentLoaded", function () {
  var host = window.location.hostname;
  document.querySelectorAll("a[href]").forEach(function (link) {
    // The ribbon brand/home link uses an absolute canonical URL as a no-JS
    // fallback (rewritten to a relative path by brand-link.js). It is internal
    // navigation, so never open it in a new tab.
    if (link.classList.contains("top-nav-brand")) return;
    var href = link.getAttribute("href");
    if (!href) return;
    var url;
    try {
      // Resolve relative URLs against the current page; compare real hostnames
      // rather than substring-matching, which can misclassify links.
      url = new URL(href, window.location.href);
    } catch (e) {
      return;
    }
    if (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.hostname !== host
    ) {
      link.setAttribute("target", "_blank");
      link.setAttribute("rel", "noopener noreferrer");
    }
  });
});

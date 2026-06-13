// Point the ribbon brand/logo at the documentation root *relative to the current
// page*, so it works under any hosting base path — Read the Docs (/en/latest/),
// a GitHub Pages project subpath (/spark-operator/), or a local build — without
// hardcoding an absolute URL. Sphinx exposes the per-page root via the
// `data-content_root` attribute on the documentation_options script tag.
document.addEventListener("DOMContentLoaded", function () {
  var brand = document.querySelector(".top-nav-brand");
  if (!brand) return;
  var opts = document.querySelector("script[data-content_root]");
  var root = opts ? opts.getAttribute("data-content_root") : "./";
  brand.setAttribute("href", root + "index.html");
});

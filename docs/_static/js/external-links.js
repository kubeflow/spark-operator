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

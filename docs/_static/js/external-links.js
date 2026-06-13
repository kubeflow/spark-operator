document.addEventListener("DOMContentLoaded", function () {
  var host = window.location.hostname;
  document.querySelectorAll("a[href]").forEach(function (link) {
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

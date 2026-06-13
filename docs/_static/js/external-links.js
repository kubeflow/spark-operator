document.addEventListener("DOMContentLoaded", function () {
  var host = window.location.hostname;
  document.querySelectorAll("a[href]").forEach(function (link) {
    var href = link.getAttribute("href");
    if (href && (href.startsWith("http://") || href.startsWith("https://")) && !href.includes(host)) {
      link.setAttribute("target", "_blank");
      link.setAttribute("rel", "noopener noreferrer");
    }
  });
});

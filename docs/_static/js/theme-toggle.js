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

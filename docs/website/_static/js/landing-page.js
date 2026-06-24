document.addEventListener("DOMContentLoaded", function () {
  var landing = document.querySelector(".landing-page");
  if (!landing) return;

  document.body.classList.add("is-landing");

  var observer = new IntersectionObserver(
    function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          entry.target.classList.add("visible");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.15 }
  );

  landing
    .querySelectorAll(
      ".feature-card, .community-card, .stat, .framework-chip, .code-block-wrapper"
    )
    .forEach(function (el) {
      el.classList.add("animate-on-scroll");
      observer.observe(el);
    });
});

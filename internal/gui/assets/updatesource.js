// Settings › Network's Update source (RootSir and Sakugar_ on X): where an
// update is read and downloaded from, one menu button as the language's is.
// app.js imports this when the page is drawn, so it weighs nothing on the
// core. "" is automatic; site never goes to GitHub.
//
// The fork offers no "GitHub only": its feed is an asset of its own GitHub
// release, so GitHub and the feed are one route here, and the list GitHub's
// SHA256SUMS describes is upstream's — the official app, which carries no
// receive monitor. Offering the choice would either replace this build with
// that one, or, since the fork publishes no signed list, fail every check.
// A setting left at it by an official build reads as automatic and is
// written back as one.
window.updateSourcePick = function updateSourcePick(s, keep) {
  const names = { "": "Automatic", site: "usemagpie.ai only" };
  const b = $("#updateSourcePick");
  const chosen = s.updateSource in names ? s.updateSource : "";
  b.dataset.value = chosen;
  b.setAttribute("aria-label", t("Update source"));
  b.replaceChildren(el("span", "", t(names[chosen])), svg(CHEV, 11, 1.6));
  b.onclick = (e) => {
    e.stopPropagation();
    if (b.classList.contains("open")) return closeProtoMenu();
    openProtoMenu(b, Object.entries(names).map(([v, name]) => ({ v, name, note: "" })), chosen, (v) => {
      if (v !== chosen) savePrefs({ ...keep, updateSource: v });
    }, "Update source", "sess-menu", "right");
  };
};

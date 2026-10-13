// Per-visitor marks and notes (kept in this browser only), and the "new jobs" banner.

const STORE_KEY = "rtjobs.marks.v1"; // { "<job id>": { status, note } }
const STATUSES = ["saved", "applied", "hidden"];

function loadMarks() {
  try {
    const value = JSON.parse(localStorage.getItem(STORE_KEY) || "{}");
    return value && typeof value === "object" ? value : {};
  } catch (_) {
    return {}; // private mode / blocked storage: start empty
  }
}

function saveMarks(marks) {
  try {
    localStorage.setItem(STORE_KEY, JSON.stringify(marks));
    return true;
  } catch (_) {
    return false;
  }
}

function updateMark(jobId, patch) {
  const marks = loadMarks();
  const next = { ...(marks[jobId] || {}), ...patch };
  if (!next.status && !next.note) delete marks[jobId];
  else marks[jobId] = next;
  return saveMarks(marks) ? next : null;
}

function idsWith(status) {
  const marks = loadMarks();
  return Object.keys(marks).filter((id) => marks[id].status === status).join(",");
}

const view = new URLSearchParams(location.search);
const viewStatus = view.get("status") || "";

// A bookmarked /?status=saved has no ids yet: add this browser's list.
if (STATUSES.includes(viewStatus) && !view.has("ids")) {
  view.set("ids", idsWith(viewStatus));
  location.replace(`/?${view}`);
}

function paint() {
  const marks = loadMarks();
  document.querySelectorAll(".marks[data-job]").forEach((box) => {
    const status = (marks[box.dataset.job] || {}).status || "";
    box.dataset.status = status;
    const card = box.closest(".card");
    if (!card) return;
    card.className = card.className.replace(/status-\S+/, `status-${status || "none"}`);
    if (status === "hidden" && viewStatus !== "hidden") card.remove();
  });
  const counts = { saved: 0, applied: 0, hidden: 0 };
  Object.values(marks).forEach((m) => { if (m.status in counts) counts[m.status] += 1; });
  document.querySelectorAll("[data-count]").forEach((el) => {
    const n = counts[el.dataset.count] || 0;
    if (el.dataset.label) el.textContent = `${el.dataset.label} (${n})`;
    else el.textContent = n;
  });
}
paint();

document.addEventListener("click", (event) => {
  const link = event.target.closest('a[href^="/?status="]');
  if (link) {
    event.preventDefault();
    const status = new URLSearchParams(link.getAttribute("href").slice(2)).get("status");
    location.href = `/?status=${status}&ids=${idsWith(status)}`;
    return;
  }
  const button = event.target.closest(".marks button[data-set]");
  if (!button) return;
  const box = button.closest(".marks");
  const wanted = button.dataset.set;
  const next = box.dataset.status === wanted ? "" : wanted; // second click clears
  if (!updateMark(box.dataset.job, { status: next })) {
    alert("المتصفح مانع الحفظ (ممكن تكون في وضع التصفح الخفي)");
    return;
  }
  const card = box.closest(".card");
  if (card && next === "hidden" && viewStatus !== "hidden") {
    card.classList.add("fading");
    setTimeout(() => { card.remove(); paint(); }, 200);
  } else {
    paint();
  }
});

const filters = document.querySelector("form.filters");
if (filters) {
  filters.addEventListener("submit", () => {
    filters.elements.ids.value = idsWith(filters.elements.status.value);
    // Leave empty fields out of the URL.
    [...filters.elements].forEach((el) => { if (el.name && !el.value) el.disabled = true; });
  });
}

const note = document.getElementById("note");
if (note) {
  note.value = (loadMarks()[note.dataset.job] || {}).note || "";
  let timer;
  const hint = document.querySelector(".saved-hint");
  note.addEventListener("input", () => {
    clearTimeout(timer);
    hint.hidden = true;
    timer = setTimeout(() => {
      hint.hidden = !updateMark(note.dataset.job, { note: note.value.slice(0, 5000) });
    }, 400);
  });
}

const banner = document.getElementById("new-banner");
if (banner) {
  const since = Number(banner.dataset.latest || 0);
  const baseTitle = document.title;
  const check = async () => {
    try {
      const res = await fetch(`/api/latest?since=${since}`);
      const { new: count } = await res.json();
      if (count > 0) {
        banner.querySelector("span").textContent = count;
        banner.hidden = false;
        document.title = `(${count}) ${baseTitle}`;
      }
    } catch (_) { /* offline: try again next tick */ }
  };
  setInterval(check, 60_000);
}

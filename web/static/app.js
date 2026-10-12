// Mark buttons, notes autosave, and the "new jobs" banner.

async function postMark(jobId, body) {
  const res = await fetch(`/api/jobs/${jobId}/mark`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`mark failed: ${res.status}`);
  return res.json();
}

document.addEventListener("click", async (event) => {
  const button = event.target.closest(".marks button[data-set]");
  if (!button) return;
  const box = button.closest(".marks");
  const wanted = button.dataset.set;
  const next = box.dataset.status === wanted ? "" : wanted; // second click clears
  try {
    const { status } = await postMark(box.dataset.job, { status: next });
    box.dataset.status = status || "";
    const card = box.closest(".card");
    if (card) {
      card.className = card.className.replace(/status-\S+/, `status-${status || "none"}`);
      const filter = new URLSearchParams(location.search).get("status") || "";
      if (status === "hidden" && filter !== "hidden") {
        card.classList.add("fading");
        setTimeout(() => card.remove(), 200);
      }
    }
  } catch (err) {
    alert("محصلش حفظ، جرّب تاني");
  }
});

const note = document.getElementById("note");
if (note) {
  let timer;
  const hint = document.querySelector(".saved-hint");
  note.addEventListener("input", () => {
    clearTimeout(timer);
    hint.hidden = true;
    timer = setTimeout(async () => {
      await postMark(note.dataset.job, { note: note.value });
      hint.hidden = false;
    }, 600);
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

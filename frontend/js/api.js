// Helper bersama untuk semua halaman
const API = {
  async get(path, params = {}) {
    const qs = new URLSearchParams(
      Object.entries(params).filter(([, v]) => v !== undefined && v !== null && v !== "")
    ).toString();
    const res = await fetch(`/api${path}${qs ? "?" + qs : ""}`);
    if (!res.ok) throw new Error(await errorText(res));
    return res.json();
  },
  async post(path, body) {
    const res = await fetch(`/api${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(await errorText(res));
    return res.json();
  },
};

async function errorText(res) {
  try {
    const j = await res.json();
    if (Array.isArray(j.detail)) return j.detail.map((d) => `${d.loc.at(-1)}: ${d.msg}`).join("; ");
    return j.detail || res.statusText;
  } catch {
    return res.statusText;
  }
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// Highlight dari ES hanya berisi tag <mark>; escape semua lalu kembalikan tag mark saja.
function safeHighlight(s) {
  return esc(s).replace(/&lt;(\/?)mark&gt;/g, "<$1mark>");
}

const fmtNum = (n) => new Intl.NumberFormat("id-ID").format(n ?? 0);
const fmtDate = (s) => (s ? new Date(s).toLocaleString("id-ID", { dateStyle: "medium", timeStyle: "short" }) : "");

function formData(form) {
  const out = {};
  for (const [k, v] of new FormData(form).entries()) if (v !== "") out[k] = v;
  return out;
}

function setMsg(el, text, ok = true) {
  el.textContent = text;
  el.className = "msg " + (ok ? "ok" : "err");
}

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// tandai menu aktif
document.addEventListener("DOMContentLoaded", () => {
  const page = location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll("header.nav nav a").forEach((a) => {
    if (a.getAttribute("href") === page) a.classList.add("active");
  });
});

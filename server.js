// Unknown — anonymous 1-on-1 random chat server (MVP)
// No login, no personal data stored. Users are matched (by shared interest if possible) and relayed messages.

const http = require("http");
const fs = require("fs");
const path = require("path");
const https = require("https");
const { WebSocketServer } = require("ws");
const db = require("./db");

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, "public");

// ---- Simple static file server for the web UI ----
const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

const server = http.createServer((req, res) => {
  let urlPath = req.url.split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";

  const filePath = path.join(PUBLIC_DIR, path.normalize(urlPath).replace(/^(\.\.[\/\\])+/, ""));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      return res.end("Not found");
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(data);
  });
});

// ---- WebSocket matchmaking ----
const wss = new WebSocketServer({ server });

// Pool of sockets currently searching for a partner
let waiting = [];

// Registry of anonymous device IDs -> socket, for friend routing & presence
const registry = new Map();

// ---- Abuse protection ----
// Mobile carriers use CGNAT — many real users share one public IP — so this cap must be
// high enough not to block them, while still stopping an extreme single-IP flood.
const MAX_CONN_PER_IP = 120;
const ipConn = new Map();          // ip -> connection count
const MSG_LIMIT = 25, MSG_WINDOW = 10000; // 25 heavy messages / 10s per socket
const HEAVY = new Set(["chat", "enc", "photo", "voice", "gif", "rtc", "friendReq", "report", "connectFriend", "wipe"]);
function rateLimited(ws) {
  const now = Date.now();
  ws._msgs = (ws._msgs || []).filter((t) => now - t < MSG_WINDOW);
  ws._msgs.push(now);
  return ws._msgs.length > MSG_LIMIT;
}
function clientIp(req) {
  try {
    // Render appends the real client IP as the LAST value; leftmost entries are client-spoofable.
    const xff = (req.headers["x-forwarded-for"] || "").split(",").map((s) => s.trim()).filter(Boolean);
    return xff.length ? xff[xff.length - 1] : ((req.socket && req.socket.remoteAddress) || "?");
  } catch { return "?"; }
}
const lastSeen = new Map();      // uid -> timestamp (updated on activity/disconnect)
const showLastSeenPref = new Map(); // uid -> boolean (false = user hid last seen)

// ---- Admin / moderation state (metadata only — chat content stays E2E encrypted) ----
const ADMIN_KEY = process.env.ADMIN_KEY || "unknown-admin-2024";
const admins = new Set();          // admin sockets
const bannedUids = new Set();      // permanently blocked device ids
const reportCounts = new Map();    // uid -> number of reports received
const AUTO_BAN_AT = 5;             // 5+ reports = auto ban
// Drug words are blocked; alcohol & cigarette are intentionally allowed per config.
let bannedWords = ["ganja", "weed", "charas", "hashish", "cocaine", "heroin", "mdma", "lsd", "ecstasy", "opium", "afeem", "smack", "nasha", "nashe", "nashedi", "drugs", "brown sugar"];

function computeStats() {
  let online = 0, f = 0, m = 0, unknown = 0, inChat = 0; const orient = {};
  registry.forEach((w) => { if (w.readyState === w.OPEN) { online++; if (w.myGender === "f") f++; else if (w.myGender === "m") m++; else unknown++; const o = w.orientation || "unknown"; orient[o] = (orient[o] || 0) + 1; if (w.partner) inChat++; } });
  return { online, f, m, unknown, orient, activeChats: Math.floor(inChat / 2), waiting: waiting.length, banned: bannedUids.size };
}
function userList() { const arr = []; registry.forEach((w, uid) => { if (w.readyState === w.OPEN) arr.push({ uid, name: w.name, gender: w.myGender || "?", orientation: w.orientation || "?", inChat: !!w.partner, hidden: !!w.hidden }); }); return arr.slice(0, 500); }
function reportList() { const arr = []; reportCounts.forEach((c, uid) => arr.push({ uid, count: c })); arr.sort((a, b) => b.count - a.count); return arr.slice(0, 100); }
function pushAdmin(ws) { send(ws, { type: "adminData", stats: computeStats(), users: userList(), reports: reportList(), words: bannedWords, key: undefined }); }
function broadcastWords() { const msg = { type: "bannedWords", words: bannedWords }; wss.clients.forEach((c) => { if (!c.isAdmin) send(c, msg); }); }
setInterval(() => { admins.forEach((a) => { if (a.readyState === a.OPEN) pushAdmin(a); }); }, 3000);

// ---- Persistence (Postgres via db.js if DATABASE_URL is set, else local JSON file) ----
function currentState() { return { bans: bannedUids, reports: reportCounts, words: bannedWords, lastSeen, prefs: showLastSeenPref }; }
let saveTimer = null;
function saveData() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; db.saveAll(currentState()).catch(() => {}); }, 1500);
}
async function bootPersistence() {
  await db.init();
  const d = await db.loadAll();
  if (d) {
    (d.bans || []).forEach((u) => bannedUids.add(u));
    if (d.reports) Object.entries(d.reports).forEach(([k, v]) => reportCounts.set(k, v));
    if (Array.isArray(d.words)) bannedWords = d.words;
    if (d.lastSeen) Object.entries(d.lastSeen).forEach(([k, v]) => lastSeen.set(k, v));
    if (d.prefs) Object.entries(d.prefs).forEach(([k, v]) => showLastSeenPref.set(k, v));
    console.log(`Loaded: ${bannedUids.size} banned, ${reportCounts.size} reported, ${bannedWords.length} words (${db.usingDb() ? "Postgres" : "file"})`);
  }
}
bootPersistence();
setInterval(saveData, 30000);
["SIGINT", "SIGTERM"].forEach((sig) => process.on(sig, () => { db.saveAll(currentState()).finally(() => process.exit(0)); }));

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function removeFromWaiting(ws) {
  const i = waiting.indexOf(ws);
  if (i !== -1) waiting.splice(i, 1);
}

function shareInterest(a, b) {
  if (!a.interests || !b.interests) return false;
  return a.interests.some((x) => b.interests.includes(x));
}

// Both users' gender preferences must be mutually satisfied
function genderOK(a, b) {
  const aWants = !a.wantGender || a.wantGender === "any" || a.wantGender === b.myGender;
  const bWants = !b.wantGender || b.wantGender === "any" || b.wantGender === a.myGender;
  return aWants && bWants;
}

function findMatch(ws) {
  removeFromWaiting(ws);

  const eligible = waiting.filter((w) => w.readyState === w.OPEN && genderOK(ws, w));
  // 1) Prefer someone (gender-ok) who shares an interest
  let partner = eligible.find((w) => shareInterest(ws, w));
  // 2) Otherwise take any gender-ok user
  if (!partner) partner = eligible[0];
  const idx = partner ? waiting.indexOf(partner) : -1;

  if (idx !== -1) {
    const partner = waiting.splice(idx, 1)[0];
    ws.partner = partner;
    partner.partner = ws;
    send(ws, { type: "matched" });
    send(partner, { type: "matched" });
  } else {
    waiting.push(ws);
    send(ws, { type: "waiting" });
  }
}

function leavePartner(ws, notify = true) {
  const partner = ws.partner;
  if (partner) {
    partner.partner = null;
    ws.partner = null;
    if (notify) send(partner, { type: "partnerLeft" });
  }
  removeFromWaiting(ws);
}

function identify(ws, msg) {
  if (typeof msg.uid === "string" && msg.uid) {
    ws.uid = msg.uid.slice(0, 40);
    registry.set(ws.uid, ws);
    lastSeen.set(ws.uid, Date.now());
  }
  if (typeof msg.name === "string") ws.name = msg.name.slice(0, 20) || "Someone";
  if (typeof msg.orientation === "string") ws.orientation = msg.orientation.slice(0, 20);
  if (typeof msg.showLastSeen === "boolean") { ws.showLastSeen = msg.showLastSeen; if (ws.uid) showLastSeenPref.set(ws.uid, msg.showLastSeen); }
}

// Returns true if the socket is banned (and notifies + closes it)
function enforceBan(ws) {
  if (ws.uid && bannedUids.has(ws.uid)) { send(ws, { type: "banned" }); try { ws.close(); } catch {} return true; }
  return false;
}

function applyPrefs(ws, msg) {
  identify(ws, msg);
  if (Array.isArray(msg.interests)) {
    ws.interests = msg.interests.map((s) => String(s).slice(0, 20)).slice(0, 5);
  }
  ws.myGender = msg.myGender === "f" || msg.myGender === "m" ? msg.myGender : null;
  ws.wantGender = msg.wantGender === "f" || msg.wantGender === "m" ? msg.wantGender : "any";
}

wss.on("connection", (ws, req) => {
  ws.partner = null;
  ws.interests = [];
  ws.myGender = null;
  ws.wantGender = "any";
  ws.uid = null;
  ws.name = "Someone";
  ws.hidden = false;
  ws.showLastSeen = true;
  ws.orientation = "";
  ws.isAdmin = false;
  ws._ip = clientIp(req);

  // Per-IP connection cap (basic DoS / spam protection)
  ipConn.set(ws._ip, (ipConn.get(ws._ip) || 0) + 1);
  if (ipConn.get(ws._ip) > MAX_CONN_PER_IP) { try { ws.close(); } catch {} return; }

  ws.on("message", (raw) => {
    if (raw && raw.length > 1500000) return; // hard cap on any single frame
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (HEAVY.has(msg.type) && rateLimited(ws)) return; // throttle spam/flood

    switch (msg.type) {
      case "find": {
        applyPrefs(ws, msg);
        if (enforceBan(ws)) break;
        send(ws, { type: "bannedWords", words: bannedWords });
        leavePartner(ws, true);
        findMatch(ws);
        break;
      }
      case "chat": {
        const text = String(msg.text || "").slice(0, 2000);
        if (ws.partner && text.trim()) send(ws.partner, { type: "chat", text });
        break;
      }
      case "photo": {
        // Relay a downscaled data-URL image. Hard size cap to protect the server.
        const data = String(msg.data || "");
        const source = msg.source === "live" ? "live" : "gallery";
        if (ws.partner && data.startsWith("data:image/") && data.length <= 600000) {
          send(ws.partner, { type: "photo", data, source });
        }
        break;
      }
      case "gif": {
        // Relay a GIF by URL (must be an https image from an allowed host)
        const url = String(msg.url || "");
        if (ws.partner && /^https:\/\/(media[0-9]*\.giphy\.com|media\.tenor\.com)\//.test(url) && url.length <= 400) {
          send(ws.partner, { type: "gif", url });
        }
        break;
      }
      case "voice": {
        // Relay a short voice message (data-URL audio). Hard size cap.
        const data = String(msg.data || "");
        if (ws.partner && data.startsWith("data:audio/") && data.length <= 900000) {
          send(ws.partner, { type: "voice", data });
        }
        break;
      }
      case "pubkey": {
        // Relay a public key for the end-to-end key exchange (server can't derive the shared secret)
        if (ws.partner && msg.jwk) send(ws.partner, { type: "pubkey", jwk: msg.jwk });
        break;
      }
      case "enc": {
        // Relay an end-to-end encrypted envelope. Server never sees the plaintext.
        const p = msg.p;
        if (ws.partner && p && typeof p.iv === "string" && typeof p.ct === "string" && p.ct.length <= 1400000) {
          send(ws.partner, { type: "enc", p });
        }
        break;
      }
      case "rtc": {
        // Relay WebRTC signaling (offer/answer/ICE) to the partner. Media is peer-to-peer & DTLS-encrypted.
        if (ws.partner && msg.data) send(ws.partner, { type: "rtc", data: msg.data });
        break;
      }
      case "typing": {
        if (ws.partner) send(ws.partner, { type: "partnerTyping", on: !!msg.on });
        break;
      }
      case "next": {
        applyPrefs(ws, msg);
        leavePartner(ws, true);
        findMatch(ws);
        break;
      }
      case "report": {
        const target = ws.partner;
        console.log(`[REPORT] ${new Date().toISOString()} reason=${String(msg.reason || "n/a").slice(0, 100)}`);
        if (target && target.uid) {
          const n = (reportCounts.get(target.uid) || 0) + 1;
          reportCounts.set(target.uid, n);
          if (n >= AUTO_BAN_AT) { bannedUids.add(target.uid); db.persistBan(target.uid, target._ip); send(target, { type: "banned" }); try { target.close(); } catch {} }
          saveData();
        }
        leavePartner(ws, true);
        findMatch(ws);
        break;
      }
      case "hello": {
        // Register identity without searching (used on the Friends screen)
        identify(ws, msg);
        if (typeof msg.hidden === "boolean") ws.hidden = msg.hidden;
        if (enforceBan(ws)) break;
        send(ws, { type: "bannedWords", words: bannedWords });
        break;
      }
      case "adminAuth": {
        ws._adminTries = (ws._adminTries || 0) + 1;
        if (ws._adminTries > 5) { send(ws, { type: "adminFail" }); break; } // block brute-force
        if (String(msg.key || "") === ADMIN_KEY) { ws.isAdmin = true; admins.add(ws); send(ws, { type: "adminOk" }); pushAdmin(ws); }
        else send(ws, { type: "adminFail" });
        break;
      }
      case "adminBlock": {
        if (!ws.isAdmin) break;
        const u = String(msg.uid || ""); if (u) { bannedUids.add(u); const w = registry.get(u); db.persistBan(u, w ? w._ip : null); if (w) { send(w, { type: "banned" }); try { w.close(); } catch {} } }
        saveData(); pushAdmin(ws);
        break;
      }
      case "adminUnblock": {
        if (!ws.isAdmin) break;
        const u = String(msg.uid || ""); bannedUids.delete(u); db.persistUnban(u); saveData(); pushAdmin(ws);
        break;
      }
      case "adminKick": {
        if (!ws.isAdmin) break;
        const w = registry.get(String(msg.uid || "")); if (w) { leavePartner(w, true); send(w, { type: "kicked" }); }
        break;
      }
      case "adminMsg": {
        if (!ws.isAdmin) break;
        const w = registry.get(String(msg.uid || "")); if (w) send(w, { type: "adminMsg", text: String(msg.text || "").slice(0, 500) });
        break;
      }
      case "adminSetWords": {
        if (!ws.isAdmin) break;
        if (Array.isArray(msg.words)) { bannedWords = msg.words.map((s) => String(s).toLowerCase().trim()).filter(Boolean).slice(0, 300); broadcastWords(); saveData(); pushAdmin(ws); }
        break;
      }
      case "leave": {
        // Leave the current partner but keep the connection (for presence/friends)
        leavePartner(ws, true);
        break;
      }
      case "wipe": {
        // Ask the partner to delete the saved friend-chat on their side too
        if (ws.partner) send(ws.partner, { type: "wipe" });
        break;
      }
      case "setHidden": {
        ws.hidden = !!msg.hidden; // Ghost mode (PRO): appear offline to friends
        break;
      }
      case "friendReq": {
        // Ask the current partner to be friends
        if (ws.partner) send(ws.partner, { type: "friendReq", uid: ws.uid, name: ws.name });
        break;
      }
      case "friendAccept": {
        // Confirm friendship to the current partner
        if (ws.partner) send(ws.partner, { type: "friendAccept", uid: ws.uid, name: ws.name });
        break;
      }
      case "connectFriend": {
        // Reconnect directly to a specific friend by their anonymous id
        const target = registry.get(String(msg.toUid || ""));
        if (!target || target.readyState !== target.OPEN) {
          send(ws, { type: "friendUnavailable", reason: "offline" });
        } else if (target.partner) {
          send(ws, { type: "friendUnavailable", reason: "busy" });
        } else {
          leavePartner(ws, true);
          leavePartner(target, true);
          ws.partner = target;
          target.partner = ws;
          send(ws, { type: "matched", friend: true, name: target.name, uid: target.uid });
          send(target, { type: "matched", friend: true, name: ws.name, uid: ws.uid, incoming: true });
        }
        break;
      }
      case "presence": {
        // Which of the given friend ids are online (and not in ghost mode)? Plus last-seen for offline ones.
        const uids = Array.isArray(msg.uids) ? msg.uids.slice(0, 200) : [];
        const online = [];
        const seen = {};
        uids.forEach((u) => {
          u = String(u);
          const w = registry.get(u);
          if (w && w.readyState === w.OPEN && !w.hidden) { online.push(u); }
          else if (showLastSeenPref.get(u) !== false && lastSeen.has(u)) { seen[u] = lastSeen.get(u); }
        });
        send(ws, { type: "presence", online, seen });
        break;
      }
    }
  });

  ws.on("close", () => {
    admins.delete(ws);
    ipConn.set(ws._ip, Math.max(0, (ipConn.get(ws._ip) || 1) - 1));
    leavePartner(ws, true);
    if (ws.uid) { lastSeen.set(ws.uid, Date.now()); if (registry.get(ws.uid) === ws) registry.delete(ws.uid); }
  });
  ws.on("error", () => {});
});

server.listen(PORT, () => {
  console.log(`Unknown chat server running on http://localhost:${PORT}`);
});

// Keep-alive: on Render's free tier, ping our own public URL every ~13 min so the
// instance never idles out (no cold-start delay for the first user).
if (process.env.RENDER_EXTERNAL_URL) {
  setInterval(() => {
    https.get(process.env.RENDER_EXTERNAL_URL, (r) => r.resume()).on("error", () => {});
  }, 13 * 60 * 1000);
  console.log("Keep-alive enabled for", process.env.RENDER_EXTERNAL_URL);
}

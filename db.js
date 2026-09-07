// Persistence layer for Unknown.
// If DATABASE_URL is set (e.g. Supabase free Postgres) it uses Postgres.
// Otherwise it falls back to a local JSON file so the app still runs with zero setup.

const fs = require("fs");
const path = require("path");

const DATABASE_URL = process.env.DATABASE_URL;
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "moderation.json");

let pool = null;

async function init() {
  if (!DATABASE_URL) { console.log("[db] No DATABASE_URL — using local JSON file."); return false; }
  try {
    const { Pool } = require("pg");
    pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await pool.query(`CREATE TABLE IF NOT EXISTS bans (uid TEXT PRIMARY KEY, ip TEXT, created_at BIGINT)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS reports (uid TEXT PRIMARY KEY, count INT)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS banned_words (word TEXT PRIMARY KEY)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS last_seen (uid TEXT PRIMARY KEY, ts BIGINT, show_ls BOOLEAN)`);
    console.log("[db] Connected to Postgres.");
    return true;
  } catch (e) {
    console.error("[db] Postgres init failed, falling back to JSON file:", e.message);
    pool = null;
    return false;
  }
}

function usingDb() { return !!pool; }

// Returns { bans:[uid...], reports:{uid:count}, words:[word...], lastSeen:{uid:ts}, prefs:{uid:bool} }
async function loadAll() {
  if (pool) {
    try {
      const [bans, reports, words, ls] = await Promise.all([
        pool.query("SELECT uid FROM bans"),
        pool.query("SELECT uid, count FROM reports"),
        pool.query("SELECT word FROM banned_words"),
        pool.query("SELECT uid, ts, show_ls FROM last_seen"),
      ]);
      const reportsObj = {}; reports.rows.forEach((r) => (reportsObj[r.uid] = r.count));
      const lastSeen = {}, prefs = {};
      ls.rows.forEach((r) => { lastSeen[r.uid] = Number(r.ts); if (r.show_ls === false) prefs[r.uid] = false; });
      return { bans: bans.rows.map((r) => r.uid), reports: reportsObj, words: words.rows.map((r) => r.word), lastSeen, prefs };
    } catch (e) { console.error("[db] loadAll failed:", e.message); return null; }
  }
  // JSON fallback
  try {
    const j = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    return { bans: j.bannedUids || [], reports: j.reportCounts || {}, words: j.bannedWords || null, lastSeen: j.lastSeen || {}, prefs: j.showLastSeenPref || {} };
  } catch { return { bans: [], reports: {}, words: null, lastSeen: {}, prefs: {} }; }
}

// state = { bans:Set, reports:Map, words:[], lastSeen:Map, prefs:Map }
async function saveAll(state) {
  if (pool) {
    try {
      // words
      await pool.query("DELETE FROM banned_words");
      for (const w of state.words) await pool.query("INSERT INTO banned_words(word) VALUES($1) ON CONFLICT DO NOTHING", [w]);
      // reports
      for (const [uid, count] of state.reports) await pool.query("INSERT INTO reports(uid,count) VALUES($1,$2) ON CONFLICT(uid) DO UPDATE SET count=$2", [uid, count]);
      // last seen + prefs
      for (const [uid, ts] of state.lastSeen) { const show = state.prefs.get(uid) !== false; await pool.query("INSERT INTO last_seen(uid,ts,show_ls) VALUES($1,$2,$3) ON CONFLICT(uid) DO UPDATE SET ts=$2, show_ls=$3", [uid, ts, show]); }
    } catch (e) { console.error("[db] saveAll failed:", e.message); }
    return;
  }
  // JSON fallback
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify({
      bannedUids: [...state.bans], reportCounts: Object.fromEntries(state.reports),
      bannedWords: state.words, lastSeen: Object.fromEntries(state.lastSeen), showLastSeenPref: Object.fromEntries(state.prefs),
    }));
  } catch (e) { console.error("[db] file save failed:", e.message); }
}

// Immediate ban write (so bans persist instantly even between debounced saves)
async function persistBan(uid, ip) { if (pool) { try { await pool.query("INSERT INTO bans(uid,ip,created_at) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", [uid, ip || null, Date.now()]); } catch (e) { console.error("[db] persistBan:", e.message); } } }
async function persistUnban(uid) { if (pool) { try { await pool.query("DELETE FROM bans WHERE uid=$1", [uid]); } catch (e) { console.error("[db] persistUnban:", e.message); } } }

module.exports = { init, usingDb, loadAll, saveAll, persistBan, persistUnban };

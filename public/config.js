// Backend location for the app.
//
// WEB (served by the Node server): leave this empty — the app uses the same origin.
// ANDROID APP: set this to your hosted backend's wss URL.
//
// TEMPORARY TEST TUNNEL (works only while the PC + server + cloudflared are running):
window.UNKNOWN_BACKEND = "wss://inspector-today-started-finder.trycloudflare.com";
//
// For production, replace the line above with your permanent host, e.g.:
//   window.UNKNOWN_BACKEND = "wss://unknown-xxxx.onrender.com";

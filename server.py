# Snake Showdown LAN server (Python, no installs/pip needed).  Run:  python server.py
import json, os, re, select, socket, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("PORT") or (sys.argv[1] if len(sys.argv) > 1 else 3000))
HERE = os.path.dirname(os.path.abspath(__file__))
lock = threading.Lock()
rooms = {}  # code -> {"host": id, "dirty": bool, "clients": {id: {"w": wfile, "pres": {}, "ev": Event}}}

def clean(s): return re.sub(r"[^a-z0-9]", "", (s or "").lower())[:8]

def sse(c, obj):  # call only while holding `lock`
    try:
        c["w"].write(b"data: " + json.dumps(obj).encode() + b"\n\n"); c["w"].flush()
    except Exception:
        c["ev"].set()

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass

    def reply(self, code, body=b"", ctype="text/plain"):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.end_headers()
        if body: self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        code, cid = clean(q.get("room", [""])[0]), clean(q.get("id", [""])[0])
        if u.path == "/events":
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream"); self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close"); self.end_headers()
            me = {"w": self.wfile, "pres": {}, "ev": threading.Event()}
            with lock:
                room = rooms.get(code)
                if q.get("host", [""])[0] == "1":
                    if room: sse(me, {"t": "err", "msg": "That code is already in use. Try again."}); self.close_connection = True; return
                    room = rooms[code] = {"host": cid, "dirty": False, "clients": {}}
                elif not room: sse(me, {"t": "err", "msg": "No room found with that code."}); self.close_connection = True; return
                if len(room["clients"]) >= 8: sse(me, {"t": "err", "msg": "That room is full."}); self.close_connection = True; return
                room["clients"][cid] = me; sse(me, {"t": "ok"}); room["dirty"] = True
            try:  # wait until the browser disconnects
                while not me["ev"].is_set():
                    if select.select([self.connection], [], [], 0.5)[0] and not self.connection.recv(1, socket.MSG_PEEK): break
            except Exception: pass
            with lock:
                room["clients"].pop(cid, None)
                if room["host"] == cid:
                    for c in room["clients"].values(): sse(c, {"t": "bye"}); c["ev"].set()
                    rooms.pop(code, None)
                else: room["dirty"] = True
            self.close_connection = True; return
        if u.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f: return self.reply(200, f.read(), "text/html; charset=utf-8")
        self.reply(404, b"Not found")

    def do_POST(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        code, cid = clean(q.get("room", [""])[0]), clean(q.get("id", [""])[0])
        n = min(int(self.headers.get("Content-Length") or 0), 20000)
        body = self.rfile.read(n)
        if u.path == "/p":
            try:
                patch = json.loads(body)
                with lock:
                    room = rooms.get(code); c = room and room["clients"].get(cid)
                    if c: c["pres"].update(patch); room["dirty"] = True
            except Exception: pass
        self.reply(204)

def broadcaster():  # ~25x/sec share everyone's state with each room
    last_ping = time.time()
    while True:
        time.sleep(0.04)
        with lock:
            ping = time.time() - last_ping > 5
            if ping: last_ping = time.time()
            for r in rooms.values():
                if r["dirty"]:
                    r["dirty"] = False
                    snap = {i: c["pres"] for i, c in r["clients"].items()}
                    for c in list(r["clients"].values()): sse(c, {"t": "s", "p": snap})

def lan_ips():
    ips = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(("10.255.255.255", 1)); ips.add(s.getsockname()[0]); s.close()
    except Exception: pass
    try: ips.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except Exception: pass
    return sorted(i for i in ips if not i.startswith("127."))

if __name__ == "__main__":
    threading.Thread(target=broadcaster, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), H); srv.daemon_threads = True
    print("\n  Snake Showdown is running!\n\n  On this computer:      http://localhost:%d" % PORT)
    for ip in lan_ips(): print("  Friends on your WiFi:  http://%s:%d" % (ip, PORT))
    print("\n  (Press Ctrl+C to stop)\n")
    try: srv.serve_forever()
    except KeyboardInterrupt: pass

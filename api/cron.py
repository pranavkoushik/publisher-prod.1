"""Vercel HTTP entrypoint for the scheduled publisher intel job."""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler

from publisher_intel import run_publisher_intel


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict):
        # Vercel expects a standard HTTP response, so we serialize the job result
        # into JSON for observability and easier manual testing.
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _is_authorized(self) -> bool:
        cron_secret = os.getenv("CRON_SECRET")
        if not cron_secret:
            # Authorization is optional for local/manual testing, but production
            # should set CRON_SECRET so only trusted cron calls can trigger runs.
            return True

        # Vercel can call the function with a bearer token so the endpoint is
        # not exposed to anyone who discovers the public URL.
        expected = f"Bearer {cron_secret}"
        return self.headers.get("Authorization") == expected

    def do_GET(self):
        # Vercel cron can invoke this route with GET, and we also support manual
        # browser testing with the same path when debugging deployments.
        if not self._is_authorized():
            self._send_json(401, {"ok": False, "error": "Unauthorized"})
            return

        try:
            result = run_publisher_intel()
            self._send_json(200, result)
        except Exception as exc:
            self._send_json(500, {"ok": False, "error": str(exc)})

    def do_POST(self):
        # POST support keeps the endpoint flexible for manual testing tools.
        self.do_GET()

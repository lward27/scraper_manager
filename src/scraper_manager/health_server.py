"""
Health check HTTP server for Kubernetes liveness/readiness probes.

Runs a lightweight HTTP server on port 8080 that exposes:
- GET /healthz - liveness probe (always returns 200 if process is running)
- GET /ready - readiness probe (returns 200 if not currently processing)
- GET /metrics - Prometheus metrics endpoint
"""

import asyncio
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Optional

from scraper_manager.logger import get_logger
from scraper_manager.metrics import metrics

log = get_logger(__name__)

# Global state
_is_ready = True
_server: Optional[HTTPServer] = None


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health checks and metrics."""

    def log_message(self, format, *args):
        """Suppress default logging to stderr."""
        pass

    def do_GET(self):
        if self.path == "/healthz":
            self._handle_healthz()
        elif self.path == "/ready":
            self._handle_ready()
        elif self.path == "/metrics":
            self._handle_metrics()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def _handle_healthz(self):
        """Liveness probe - always returns 200 if process is running."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        response = {"status": "ok"}
        self.wfile.write(json.dumps(response).encode())

    def _handle_ready(self):
        """Readiness probe - returns 200 if not processing."""
        self.send_response(200 if _is_ready else 503)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        response = {"ready": _is_ready}
        self.wfile.write(json.dumps(response).encode())

    def _handle_metrics(self):
        """Prometheus metrics endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(metrics.render_prometheus().encode())


def start_health_server(port: int = 8080) -> Thread:
    """
    Start the health check HTTP server in a background thread.

    Args:
        port: Port to listen on (default: 8080).

    Returns:
        Thread object for the server.
    """
    global _server

    _server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = Thread(target=_server.serve_forever, daemon=True)
    thread.start()

    log.logger.info(f"Health check server started on port {port}")
    return thread


def stop_health_server():
    """Stop the health check server."""
    global _server
    if _server:
        _server.shutdown()
        log.logger.info("Health check server stopped")


def set_ready(ready: bool):
    """Set readiness state."""
    global _is_ready
    _is_ready = ready

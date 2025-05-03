import logging
import os
import uuid
import posthog
from typing import Dict

# # Set your PostHog project API key and host (or use env vars)
# POSTHOG_API_KEY = os.getenv("SEQUOR_TELEMETRY_KEY", "<your-posthog-key>")
# POSTHOG_HOST = os.getenv("SEQUOR_TELEMETRY_HOST", "https://app.posthog.com")

# Global state
_logger_registry: Dict[str, "TelemetryLogger"] = {}
_user_id = None
_enabled = True

class TelemetryLogger:
    def __init__(self, name: str):
        self.name = name

    def event(self, name: str, **props):
        self._send(name, props)

    def _send(self, event_type: str, props: dict):
        logger = logging.getLogger("sequor.telemetry")
        if _enabled:
            try:
                data = {
                    "component": self.name,
                    "event_type": event_type,
                    **(props or {}),
                }
                logger.info(f"Before: {event_type}")
                posthog.capture(_user_id, event_type, data)
                logger.info(f"After: {event_type}")
            except Exception:
                logger.info(f"Event sending failed: {event_type}")
                # pass
        return

def _load_or_create_user_id(path: str):
    if os.path.exists(path):
        return open(path).read().strip()
    uid = str(uuid.uuid4())
    with open(path, 'w') as f:
        f.write(uid)
    return uid

def basicConfig(api_key: str, host: str, user_id_file: StopIteration, enabled: bool = True):
    global _user_id, _enabled
    posthog.project_api_key = api_key
    posthog.host = host
    _enabled = enabled
    _user_id = _load_or_create_user_id(user_id_file)

def getLogger(name: str) -> TelemetryLogger:
    if name not in _logger_registry:
        _logger_registry[name] = TelemetryLogger(name)
    return _logger_registry[name]
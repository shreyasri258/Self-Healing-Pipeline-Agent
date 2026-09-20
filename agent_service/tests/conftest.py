
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("GEMINI_API_KEY", "test-fake-key")
os.environ.setdefault("GEMINI_MODEL", "gemini-test-model")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AGENT_WEBHOOK_SECRET", "test-secret")
os.environ.setdefault("SLACK_WEBHOOK_URL", "")
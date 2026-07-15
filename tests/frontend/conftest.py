"""Shared fixtures for frontend E2E tests."""

import os
import subprocess
import time
import pytest
from playwright.sync_api import sync_playwright

PORT = 8111
BASE_URL = f"http://localhost:{PORT}"
REPORT_FILE = "index.html"
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Serve the regulation directory itself, not the repo root, so index.html sits
# next to its siblings (comment_detail.json, read-the-rule.html) exactly as it is
# deployed to Netlify. Serving from the root would break the report's relative
# fetch('comment_detail.json') and the Read-the-Rule link. Override the fixture
# regulation with TEST_REGULATION=<slug>.
REGULATION = os.environ.get("TEST_REGULATION", "omb-financial-assistance")
SERVE_DIR = os.path.join(ROOT_DIR, "regulations", REGULATION)


@pytest.fixture(scope="session")
def server():
    """Start a local HTTP server for the duration of the test session."""
    if not os.path.exists(os.path.join(SERVE_DIR, REPORT_FILE)):
        pytest.skip(
            f"No {REPORT_FILE} in {SERVE_DIR}; generate the report first "
            f"(python generate_report.py ... --output index.html)."
        )
    proc = subprocess.Popen(
        ["python3", "-m", "http.server", str(PORT)],
        cwd=SERVE_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    yield BASE_URL
    proc.terminate()
    proc.wait()


@pytest.fixture(scope="session")
def browser_ctx():
    """Shared browser context for all tests."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        yield context
        context.close()
        browser.close()


@pytest.fixture
def page(server, browser_ctx):
    """Fresh page pointed at the report for each test."""
    pg = browser_ctx.new_page()
    pg.goto(f"{server}/{REPORT_FILE}", wait_until="networkidle", timeout=60000)
    # Wait for DataTable to initialize
    pg.wait_for_selector("#commentsTable_wrapper", timeout=10000)
    yield pg
    pg.close()

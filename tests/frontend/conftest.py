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


@pytest.fixture(scope="session")
def server():
    """Start a local HTTP server for the duration of the test session."""
    proc = subprocess.Popen(
        ["python3", "-m", "http.server", str(PORT)],
        cwd=ROOT_DIR,
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

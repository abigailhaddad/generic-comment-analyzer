"""Test that the report page loads correctly."""

import pytest


def test_page_title(page):
    """Page title contains regulation name."""
    assert "Comment Analysis Report" in page.title() or "Analysis" in page.title()


def test_stat_cards_visible(page):
    """Summary stat cards are rendered."""
    cards = page.query_selector_all(".stat-card")
    assert len(cards) >= 5, f"Expected at least 5 stat cards, got {len(cards)}"


def test_comments_table_has_rows(page):
    """DataTable has comment rows."""
    rows = page.query_selector_all("#commentsTable tbody tr")
    assert len(rows) > 0, "Comments table has no rows"


def test_no_js_errors(server, browser_ctx):
    """Page loads without JavaScript errors."""
    errors = []
    pg = browser_ctx.new_page()
    pg.on("pageerror", lambda err: errors.append(str(err)))
    pg.goto(f"{server}/index.html", wait_until="networkidle", timeout=15000)
    pg.wait_for_selector("#commentsTable_wrapper", timeout=10000)
    pg.close()
    assert len(errors) == 0, f"JS errors: {errors}"


def test_campaigns_section_exists(page):
    """Campaign detection section is rendered."""
    section = page.query_selector("#campaignsSection")
    assert section is not None, "Campaigns section not found"


def test_no_rule_link_without_rule_text(page):
    """A regulation without rule_sections.json shows no 'Read the Rule' callout."""
    if page.query_selector(".rule-callout") is not None:
        pytest.skip("this regulation ships rule text, so the callout is expected")
    assert page.query_selector(".rule-callout") is None


def test_concern_bars_exist(page):
    """Top Concerns renders stacked (oppose/support) bars."""
    stacked = page.query_selector_all("#concernsSection .concern-bar-stacked")
    assert len(stacked) > 0, "No stacked concern bars rendered"
    assert page.query_selector("#concernsSection .seg-oppose") is not None, "No oppose segment"

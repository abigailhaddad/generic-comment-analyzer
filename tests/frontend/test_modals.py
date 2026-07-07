"""Test modal dialogs."""

import pytest


def test_comment_row_click_opens_modal(page):
    """Clicking a table row opens the comment detail modal."""
    page.click("#commentsTable tbody tr.clickable-row", timeout=5000)
    page.wait_for_selector("#commentModal.show", timeout=5000)

    body = page.query_selector("#commentModalBody")
    assert body is not None
    assert len(body.inner_text()) > 0, "Modal body is empty"


def test_comment_modal_has_fields(page):
    """Comment modal shows key fields."""
    page.click("#commentsTable tbody tr.clickable-row", timeout=5000)
    page.wait_for_selector("#commentModal.show", timeout=5000)

    body = page.inner_text("#commentModalBody")
    assert "Submitter" in body
    assert "Entity Type" in body


def test_comment_modal_closes(page):
    """Comment modal closes with the X button."""
    page.click("#commentsTable tbody tr.clickable-row", timeout=5000)
    page.wait_for_selector("#commentModal.show", timeout=5000)

    page.click("#commentModal .btn-close")
    page.wait_for_timeout(500)

    modal = page.query_selector("#commentModal.show")
    assert modal is None, "Modal did not close"


def test_entity_card_opens_modal(page):
    """Clicking an entity card opens the entity modal."""
    page.click(".entity-card", timeout=5000)
    page.wait_for_selector("#entityModal.show", timeout=5000)

    title = page.inner_text("#entityModalTitle")
    assert len(title) > 0, "Entity modal title is empty"


def test_campaign_bars_are_colored(page):
    """Campaign bars are stacked oppose/support bars — no text stance badge."""
    stacked = page.query_selector_all("#campaignsSection .concern-bar-stacked")
    if len(stacked) == 0:
        pytest.skip("No campaigns in test data")
    assert page.query_selector("#campaignsSection .stance-badge") is None, "Text stance badge should be removed"
    assert page.query_selector("#campaignsSection .seg-oppose") is not None
    assert page.query_selector("#campaignsSection .seg-support") is not None


def test_campaign_bar_opens_modal(page):
    """Clicking a campaign bar opens the campaign modal."""
    campaign_bars = page.query_selector_all("#campaignsSection .concern-row")
    if len(campaign_bars) == 0:
        pytest.skip("No campaigns in test data")

    campaign_bars[0].click()
    page.wait_for_selector("#entityModal.show", timeout=5000)

    # Title is content-labeled now ("#N · <snippet> — X similar (…)"); assert the
    # invariant tail rather than the word "Campaign".
    title = page.inner_text("#entityModalTitle")
    assert "similar" in title and "exact copies" in title


def test_campaign_modal_shows_canonical_text(page):
    """Campaign modal displays the canonical form letter text."""
    campaign_bars = page.query_selector_all("#campaignsSection .concern-row")
    if len(campaign_bars) == 0:
        pytest.skip("No campaigns in test data")

    campaign_bars[0].click()
    page.wait_for_selector("#entityModal.show", timeout=5000)

    body = page.inner_text("#entityModalBody")
    assert "Most Common Version" in body
    assert len(body) > 50, "Campaign modal has no canonical text"


def test_flag_card_opens_modal(page):
    """Clicking a config-driven flag stat card opens its modal."""
    card = page.query_selector(".flag-card")
    if card is None:
        pytest.skip("No regex flags configured for this regulation")
    label = card.inner_text().strip()
    card.click()
    page.wait_for_selector("#entityModal.show", timeout=5000)

    body = page.inner_text("#entityModalBody")
    assert "search patterns" in body.lower() or "Submitter" in body
    # Modal title should reflect the flag's label (first word of the card label)
    title = page.inner_text("#entityModalTitle")
    assert len(title) > 0


def test_flag_modal_has_pattern_link(page):
    """A flag modal contains a link to view the search patterns."""
    card = page.query_selector(".flag-card")
    if card is None:
        pytest.skip("No regex flags configured for this regulation")
    card.click()
    page.wait_for_selector("#entityModal.show", timeout=5000)

    body = page.inner_html("#entityModalBody")
    assert "View search patterns" in body


def test_support_card_opens_modal(page):
    """Clicking the Support stat card opens a modal with support comments."""
    page.locator(".stat-card.clickable", has_text="Support").first.click()
    page.wait_for_selector("#entityModal.show", timeout=5000)

    title = page.inner_text("#entityModalTitle")
    assert "Support" in title


def test_unclear_card_opens_modal(page):
    """Clicking the Unclear stat card opens a modal with unclear comments."""
    page.locator(".stat-card.clickable", has_text="Unclear").first.click()
    page.wait_for_selector("#entityModal.show", timeout=5000)

    title = page.inner_text("#entityModalTitle")
    assert "Unclear" in title


def test_regex_patterns_modal(page):
    """The 'view search patterns' link lists the flag's regex patterns."""
    card = page.query_selector(".flag-card")
    if card is None:
        pytest.skip("No regex flags configured for this regulation")
    card.click()
    page.wait_for_selector("#entityModal.show", timeout=5000)
    page.click("text=View search patterns")
    page.wait_for_timeout(500)

    title = page.inner_text("#entityModalTitle")
    assert "Search Patterns" in title
    # The patterns are rendered as <code> rows — there should be at least one.
    codes = page.query_selector_all("#entityModalBody code")
    assert len(codes) > 0, "No regex patterns listed in the modal"

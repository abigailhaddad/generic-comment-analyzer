"""Test shareable URL filter sync."""

import pytest


def _open_column_filter(page, label):
    page.click("#addFilterBtn")
    page.wait_for_selector(".filter-modal .filter-option")
    page.locator(".filter-modal .filter-option", has_text=label).first.click()
    page.wait_for_selector(".filter-modal .filter-options .filter-option input[type='checkbox']")


def _apply_first_option(page):
    page.query_selector(".filter-modal .filter-options input[type='checkbox']").click()
    page.click(".filter-modal .btn-apply")


def test_filter_updates_url(page):
    """Applying a filter updates the URL parameters."""
    _open_column_filter(page, "Entity Type")
    _apply_first_option(page)

    url = page.url
    assert "entity_type=" in url, f"URL not updated with filter: {url}"


def test_url_params_restore_filters(server, browser_ctx):
    """Opening a URL with filter params applies those filters."""
    pg = browser_ctx.new_page()
    pg.goto(f"{server}/index.html?entity_type=Attorney%2FLawyer", wait_until="networkidle", timeout=15000)
    pg.wait_for_selector("#commentsTable_wrapper", timeout=10000)

    chips = pg.query_selector_all(".filter-chip")
    assert len(chips) > 0, "URL params did not restore filter chips"

    chip_text = chips[0].inner_text()
    assert "Attorney" in chip_text, f"Wrong filter restored: {chip_text}"
    pg.close()


def test_search_param_in_url(page):
    """The Comment-text filter updates the URL with the q param."""
    page.click("#addFilterBtn")
    page.wait_for_selector(".filter-modal .filter-option")
    page.locator(".filter-modal .filter-option", has_text="Comment text").first.click()
    page.wait_for_selector(".filter-modal .filter-text-input")
    page.fill(".filter-modal .filter-text-input", "oppose")
    page.click(".filter-modal .btn-apply")
    page.wait_for_timeout(300)

    url = page.url
    assert "q=oppose" in url, f"Search not in URL: {url}"


def test_clearing_filters_cleans_url(page):
    """Removing all filters clears URL params."""
    _open_column_filter(page, "Entity Type")
    _apply_first_option(page)
    assert "entity_type=" in page.url

    page.click(".filter-chip .filter-chip-remove")
    page.wait_for_timeout(300)

    assert "entity_type=" not in page.url

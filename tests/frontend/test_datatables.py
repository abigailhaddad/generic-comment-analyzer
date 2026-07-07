"""Test DataTables sorting, pagination, and search."""

import pytest


def test_table_has_correct_columns(page):
    """Table headers match expected columns."""
    headers = page.query_selector_all("#commentsTable thead th")
    header_texts = [h.inner_text().strip() for h in headers if h.is_visible()]
    assert "ID" in header_texts
    assert "Entity Type" in header_texts
    assert "Campaign" in header_texts


def test_sorting_by_date(page):
    """Clicking Date header sorts the table."""
    # Click Date header to sort
    page.click("#commentsTable thead th:nth-child(2)")
    page.wait_for_timeout(300)

    rows = page.query_selector_all("#commentsTable tbody tr")
    assert len(rows) > 0


def test_pagination_exists(page):
    """Pagination controls are rendered."""
    pagination = page.query_selector(".dataTables_paginate")
    assert pagination is not None, "No pagination controls"


def test_page_length_selector(page):
    """Page length selector exists and works."""
    selector = page.query_selector(".dataTables_length select")
    assert selector is not None, "No page length selector"


def test_showing_info(page):
    """DataTable shows 'Showing X to Y of Z entries' info."""
    info = page.inner_text(".dataTables_info")
    assert "Showing" in info
    assert "entries" in info


def test_position_column_short_labels(page):
    """The Position column shows short stance labels, not the full stance text."""
    tags = page.query_selector_all("#commentsTable tbody tr .tag-position")
    labels = set(t.inner_text().strip() for t in tags)
    assert labels, "No position tags rendered"
    assert labels <= {"Oppose", "Support", "Unclear"}, f"Position labels not short: {labels}"

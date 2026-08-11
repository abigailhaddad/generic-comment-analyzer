"""Test filter functionality (the "+ Add Filter" flow)."""

from urllib.parse import quote

import pytest
from playwright.sync_api import TimeoutError as PlaywrightTimeout


# Filters the picker offers for EVERY regulation (not gated by config).
# Date/Submitter/Organization come from columns every report has, so they are
# always offered — unlike the config-gated ones below.
ALWAYS_FILTER_COLUMNS = ["Campaign", "Attachment", "Comment text",
                         "Date", "Submitter", "Organization"]
# Filters offered only when the regulation's config surfaces the field
# (show: [filter]). A given regulation may surface none, some, or all of these,
# so tests that need one skip when it is absent.
OPTIONAL_FILTER_COLUMNS = ["Entity Type", "State", "Position", "Concern", "Political"]


def _open_column_picker(page):
    """Click '+ Add Filter' and wait for the column-picker popover."""
    page.click("#addFilterBtn")
    page.wait_for_selector(".filter-modal .filter-option")


def _picker_labels(page):
    """Open the picker and return the offered column labels (picker left open)."""
    _open_column_picker(page)
    return [o.inner_text().strip()
            for o in page.query_selector_all(".filter-modal .filter-option")]


def _close_modal(page):
    """Dismiss any open filter modal (Escape is wired to close it)."""
    page.keyboard.press("Escape")


def _open_multiselect_filter(page, label):
    """Open the picker and drill into one multiselect column's dialog."""
    _open_column_picker(page)
    page.locator(".filter-modal .filter-option", has_text=label).first.click()
    page.wait_for_selector(".filter-modal .filter-options .filter-option input[type='checkbox']")


def _open_multiselect_or_skip(page, label):
    """Drill into a multiselect column's dialog, or skip the test if this
    regulation does not surface that column as a filter."""
    _open_column_picker(page)
    opt = page.locator(".filter-modal .filter-option", has_text=label)
    if opt.count() == 0:
        _close_modal(page)
        pytest.skip(f"'{label}' filter not surfaced by this regulation")
    opt.first.click()
    page.wait_for_selector(".filter-modal .filter-options .filter-option input[type='checkbox']")


def _open_searchable_multiselect_or_skip(page):
    """Open the first multiselect filter that shows a search box (>12 options),
    leaving its dialog open. Skips if the regulation has no such filter."""
    for label in _picker_labels(page):
        _close_modal(page)
        _open_column_picker(page)
        page.locator(".filter-modal .filter-option", has_text=label).first.click()
        try:
            page.wait_for_selector(
                ".filter-modal .filter-options .filter-option input[type='checkbox']",
                timeout=2000)
        except PlaywrightTimeout:
            _close_modal(page)  # a text filter (Comment text) has no checkboxes
            continue
        search = page.query_selector(".filter-modal .filter-search")
        if search and search.is_visible():
            return label
        _close_modal(page)
    pytest.skip("no multiselect filter with a searchable (>12) option list")


def _apply_first_option(page):
    """Check the first value in an open per-column dialog and apply it."""
    page.query_selector(".filter-modal .filter-options input[type='checkbox']").click()
    page.click(".filter-modal .btn-apply")


def test_add_filter_button_exists(page):
    """The '+ Add Filter' button is present."""
    btn = page.query_selector("#addFilterBtn")
    assert btn is not None
    assert btn.is_visible()


def test_no_standalone_search_box(page):
    """The standalone comment-search box is gone; search is a filter now."""
    assert page.query_selector("#commentSearch") is None


def test_filter_picker_lists_base_columns(page):
    """The picker always lists the universal columns and at least one config column."""
    labels = _picker_labels(page)
    for col in ALWAYS_FILTER_COLUMNS:
        assert col in labels, f"Missing always-on filter column: {col} (got {labels})"
    # A real regulation should surface at least one config-driven column too.
    present_optional = [c for c in OPTIONAL_FILTER_COLUMNS if c in labels]
    assert present_optional, f"No config-driven filter columns surfaced (got {labels})"


def test_open_column_filter_shows_options(page):
    """Opening a column filter shows checkbox options."""
    _open_multiselect_filter(page, "Entity Type")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    assert len(options) > 0, "No filter options rendered"


def test_apply_filter_creates_chip(page):
    """Applying a filter creates a chip and filters the table."""
    _open_multiselect_filter(page, "Entity Type")
    _apply_first_option(page)
    chips = page.query_selector_all(".filter-chip")
    assert len(chips) > 0, "No filter chip created"


def test_clear_filter_removes_chip(page):
    """Removing a filter chip restores the table."""
    _open_multiselect_filter(page, "Entity Type")
    _apply_first_option(page)
    page.click(".filter-chip .filter-chip-remove")
    page.wait_for_timeout(200)
    chips = page.query_selector_all(".filter-chip")
    assert len(chips) == 0, "Chip not removed"


def test_comment_text_filter_works(page):
    """The 'Comment text' filter searches the comment body and makes a chip."""
    initial_rows = len(page.query_selector_all("#commentsTable tbody tr"))
    _open_column_picker(page)
    page.locator(".filter-modal .filter-option", has_text="Comment text").first.click()
    page.wait_for_selector(".filter-modal .filter-text-input")
    page.fill(".filter-modal .filter-text-input", "oppose")
    page.click(".filter-modal .btn-apply")
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert any("Comment text" in c.inner_text() for c in chips), "No comment-text chip"
    filtered_rows = len(page.query_selector_all("#commentsTable tbody tr"))
    assert filtered_rows <= initial_rows


def test_copy_link_button_exists(page):
    """Copy Link button is present."""
    btn = page.query_selector("#copyLinkBtn")
    assert btn is not None
    assert btn.is_visible()


def test_download_csv_button_exists(page):
    """Download CSV button is present."""
    btn = page.query_selector("#downloadCsvBtn")
    assert btn is not None
    assert btn.is_visible()


def test_state_filter_has_options(page):
    """State filter shows state abbreviation checkboxes (when surfaced)."""
    _open_multiselect_or_skip(page, "State")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    labels = [o.inner_text().strip() for o in options]
    assert len(labels) > 10, f"Expected many states, got {len(labels)}"
    joined = " ".join(labels)
    assert "CA" in joined or "NY" in joined, f"Common states missing: {labels[:10]}"


def test_state_filter_applies(page):
    """Applying a state filter creates a chip and filters the table."""
    _open_multiselect_or_skip(page, "State")
    _apply_first_option(page)
    chips = page.query_selector_all(".filter-chip")
    assert len(chips) > 0, "No chip created for state filter"


def test_attachment_filter_has_yes(page):
    """Attachment filter has a 'Yes' option."""
    _open_multiselect_filter(page, "Attachment")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    labels = [o.inner_text().strip() for o in options]
    assert any(lbl.startswith("Yes") for lbl in labels), f"No Yes option: {labels}"


def test_attachment_filter_applies(page):
    """Applying attachment filter creates an Attachment chip."""
    _open_multiselect_filter(page, "Attachment")
    _apply_first_option(page)
    chips = page.query_selector_all(".filter-chip")
    assert any("Attachment" in c.inner_text() for c in chips)


def test_political_filter_has_options(page):
    """Political filter shows affiliation options (when surfaced)."""
    _open_multiselect_or_skip(page, "Political")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    assert len(options) > 0, "No political affiliation options"


def test_political_filter_applies(page):
    """Applying political filter creates a chip."""
    _open_multiselect_or_skip(page, "Political")
    _apply_first_option(page)
    chips = page.query_selector_all(".filter-chip")
    assert any("Political" in c.inner_text() for c in chips)


def test_flag_stat_cards_exist(page):
    """Config-driven regex-flag stat cards are rendered."""
    cards = page.query_selector_all(".flag-card")
    assert len(cards) > 0, "No flag stat cards rendered"


def test_flag_filter_columns_present_and_applies(page):
    """Config regex-flag columns are appended to the picker as Yes/No multiselects."""
    # The report exposes its flag definitions; use them so this stays regulation-agnostic.
    flag_labels = page.evaluate(
        "() => (typeof flagMeta !== 'undefined' ? flagMeta.map(f => f.label) : [])")
    if not flag_labels:
        pytest.skip("regulation defines no regex flags")
    _open_multiselect_or_skip(page, flag_labels[0])
    labels = [o.inner_text().strip() for o in page.query_selector_all(".filter-modal .filter-options .filter-option")]
    assert any(l.startswith("Yes") for l in labels), f"Flag filter not Yes/No: {labels}"
    assert any(l.startswith("No") for l in labels), f"Flag filter not Yes/No: {labels}"
    page.query_selector(".filter-modal .filter-options input[type='checkbox']").click()
    page.click(".filter-modal .btn-apply")
    chips = page.query_selector_all(".filter-chip")
    assert len(chips) > 0, "Applying a flag filter created no chip"


def test_select_all_checks_all_options(page):
    """The 'Select all' checkbox checks every visible option."""
    _open_searchable_multiselect_or_skip(page)
    page.query_selector(".filter-modal .selectall-cb").click()
    boxes = page.query_selector_all(".filter-modal .filter-options input[type='checkbox']")
    assert len(boxes) > 0
    assert all(b.is_checked() for b in boxes), "Select all did not check every option"


def test_option_search_filters_list(page):
    """Typing in the per-column search box narrows the option list."""
    _open_searchable_multiselect_or_skip(page)
    initial = len(page.query_selector_all(".filter-modal .filter-options .filter-option:visible"))
    page.fill(".filter-modal .filter-search", "zzzzzz")
    page.wait_for_timeout(200)
    filtered = len(page.query_selector_all(".filter-modal .filter-options .filter-option:visible"))
    assert filtered < initial, "Search did not narrow the option list"


def test_small_filters_hide_search(page):
    """Filters with few options hide the search box."""
    _open_multiselect_filter(page, "Attachment")  # Yes/No -> 2 options
    search = page.query_selector(".filter-modal .filter-search")
    assert not search.is_visible(), "Search should be hidden for a two-value filter"


def test_large_filters_show_search(page):
    """Filters with many options show the search box."""
    _open_searchable_multiselect_or_skip(page)  # only returns with the search box shown
    search = page.query_selector(".filter-modal .filter-search")
    assert search is not None and search.is_visible(), "Search should be visible for a large filter"


def test_concern_bar_click_filters_table(page):
    """Clicking a Top Concerns bar applies that concern as a filter chip + URL."""
    row = page.query_selector("#concernsSection .concern-row[data-concern]")
    if row is None:
        pytest.skip("no concerns for this regulation")
    initial_rows = len(page.query_selector_all("#commentsTable tbody tr"))
    row.click()
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert any("Concern" in c.inner_text() for c in chips), "No concern chip created"
    assert "concern=" in page.url, f"URL not updated: {page.url}"
    filtered_rows = len(page.query_selector_all("#commentsTable tbody tr"))
    assert filtered_rows <= initial_rows


def test_concern_bars_are_stacked(page):
    """Each concern bar has both an oppose and a support segment element."""
    if page.query_selector("#concernsSection") is None:
        pytest.skip("no concerns section for this regulation")
    assert len(page.query_selector_all("#concernsSection .concern-bar-stacked")) > 0
    assert page.query_selector("#concernsSection .seg-oppose") is not None
    assert page.query_selector("#concernsSection .seg-support") is not None


def test_sections_referenced_click_filters(page):
    """If a regex value-section (e.g. CFR sections) is present, clicking a bar filters."""
    row = page.query_selector(".value-row[data-section-key]")
    if row is None:
        pytest.skip("no regex value-section for this regulation")
    row.click()
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert len(chips) > 0, "No chip created from section bar click"


# ── Issue #7: filter by Date, Submitter, Organization ────────────────────────
#
# These three were the columns the picker did not offer. Date is low-cardinality
# so it reuses the ordinary multiselect; Submitter and Organization have nearly
# one distinct value per comment, so they are text filters instead — enumerating
# them would build a list as long as the table.


def _cell_index(page, header):
    """0-based index of the table column with this header text."""
    heads = [h.inner_text().strip() for h in page.query_selector_all("#commentsTable thead th")]
    assert header in heads, f"No '{header}' column (got {heads})"
    return heads.index(header)


def _sample_value(page, field, min_len=6):
    """A real value of `field` from the loaded row data, or skip if none is long
    enough to be a meaningful search term."""
    val = page.evaluate(
        """(f) => {
            const hit = commentData.find(c => (c[f] || '').trim().length >= %d);
            return hit ? hit[f].trim() : null;
        }""" % min_len,
        field,
    )
    if not val:
        pytest.skip(f"no {field} value of >= {min_len} chars in this regulation")
    return val


def _open_text_filter(page, label):
    """Open the picker and drill into one text column's dialog."""
    _open_column_picker(page)
    page.locator(".filter-modal .filter-option", has_text=label).first.click()
    page.wait_for_selector(".filter-modal .filter-text-input")


def test_date_filter_offers_options_and_applies(page):
    """Date is a plain multiselect: it lists days and filtering makes a chip."""
    initial_rows = len(page.query_selector_all("#commentsTable tbody tr"))
    _open_multiselect_filter(page, "Date")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    assert len(options) > 0, "Date filter listed no days"
    _apply_first_option(page)
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert any("Date" in c.inner_text() for c in chips), "No Date chip created"
    assert "date=" in page.url, f"Date filter not in URL: {page.url}"
    assert len(page.query_selector_all("#commentsTable tbody tr")) <= initial_rows


def test_submitter_filter_matches_the_submitter_column(page):
    """Filtering by Submitter keeps only rows whose Submitter cell contains the term."""
    term = _sample_value(page, "submitter")
    idx = _cell_index(page, "Submitter")
    _open_text_filter(page, "Submitter")
    page.fill(".filter-modal .filter-text-input", term)
    page.click(".filter-modal .btn-apply")
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert any("Submitter" in c.inner_text() for c in chips), "No Submitter chip"
    rows = page.query_selector_all("#commentsTable tbody tr")
    assert len(rows) > 0, f"Submitter filter for a real value '{term}' matched nothing"
    for r in rows:
        cells = r.query_selector_all("td")
        assert term.lower() in cells[idx].inner_text().lower(), \
            f"Row kept without '{term}' in its Submitter cell"


def test_organization_filter_matches_the_organization(page):
    """Organization has no column of its own, but filters on the value shown in
    the Submitter cell."""
    term = _sample_value(page, "organization")
    idx = _cell_index(page, "Submitter")
    _open_text_filter(page, "Organization")
    page.fill(".filter-modal .filter-text-input", term)
    page.click(".filter-modal .btn-apply")
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert any("Organization" in c.inner_text() for c in chips), "No Organization chip"
    rows = page.query_selector_all("#commentsTable tbody tr")
    assert len(rows) > 0, f"Organization filter for a real value '{term}' matched nothing"
    for r in rows:
        cells = r.query_selector_all("td")
        assert term.lower() in cells[idx].inner_text().lower(), \
            f"Row kept without '{term}' in its Submitter cell"


def test_row_field_text_filters_do_not_wait_on_detail(page):
    """Submitter/Organization read off rows the page already has, so their box is
    usable straight away — only Comment text pays for the detail shards."""
    for label in ("Submitter", "Organization"):
        _open_text_filter(page, label)
        inp = page.query_selector(".filter-modal .filter-text-input")
        assert inp.is_enabled(), f"{label} filter input was disabled on open"
        assert page.query_selector(".filter-modal .filter-popover p.text-muted") is None, \
            f"{label} filter showed the detail-loading notice"
        _close_modal(page)


def test_submitter_filter_survives_a_shared_url(page):
    """A copied link with a submitter term restores the chip on load."""
    term = _sample_value(page, "submitter")
    page.goto(f"{page.url.split('?')[0]}?submitter={quote(term.lower())}", wait_until="networkidle")
    page.wait_for_selector("#commentsTable_wrapper")
    page.wait_for_timeout(300)
    chips = page.query_selector_all(".filter-chip")
    assert any("Submitter" in c.inner_text() for c in chips), \
        "Submitter filter did not survive the URL round-trip"

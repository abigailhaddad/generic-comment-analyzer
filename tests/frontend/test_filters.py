"""Test filter functionality (the "+ Add Filter" flow)."""

import pytest


# Base (regulation-agnostic) columns always offered by the "+ Add Filter" picker.
# Regex-flag columns are appended per-regulation and are NOT listed here.
BASE_FILTER_COLUMNS = [
    "Entity Type", "State", "Position", "Concern",
    "Campaign", "Attachment", "Political", "Comment text",
]


def _open_column_picker(page):
    """Click '+ Add Filter' and wait for the column-picker popover."""
    page.click("#addFilterBtn")
    page.wait_for_selector(".filter-modal .filter-option")


def _open_multiselect_filter(page, label):
    """Open the picker and drill into one multiselect column's dialog."""
    _open_column_picker(page)
    page.locator(".filter-modal .filter-option", has_text=label).first.click()
    page.wait_for_selector(".filter-modal .filter-options .filter-option input[type='checkbox']")


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
    """The column picker lists every base column."""
    _open_column_picker(page)
    options = page.query_selector_all(".filter-modal .filter-option")
    labels = [o.inner_text().strip() for o in options]
    for col in BASE_FILTER_COLUMNS:
        assert col in labels, f"Missing filter column: {col} (got {labels})"


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
    """State filter shows state abbreviation checkboxes."""
    _open_multiselect_filter(page, "State")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    labels = [o.inner_text().strip() for o in options]
    assert len(labels) > 10, f"Expected many states, got {len(labels)}"
    joined = " ".join(labels)
    assert "CA" in joined or "NY" in joined, f"Common states missing: {labels[:10]}"


def test_state_filter_applies(page):
    """Applying a state filter creates a chip and filters the table."""
    _open_multiselect_filter(page, "State")
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
    """Political filter shows affiliation options."""
    _open_multiselect_filter(page, "Political")
    options = page.query_selector_all(".filter-modal .filter-options .filter-option")
    assert len(options) > 0, "No political affiliation options"


def test_political_filter_applies(page):
    """Applying political filter creates a chip."""
    _open_multiselect_filter(page, "Political")
    _apply_first_option(page)
    chips = page.query_selector_all(".filter-chip")
    assert any("Political" in c.inner_text() for c in chips)


def test_flag_stat_cards_exist(page):
    """Config-driven regex-flag stat cards are rendered."""
    cards = page.query_selector_all(".flag-card")
    assert len(cards) > 0, "No flag stat cards rendered"


def test_flag_filter_columns_present_and_applies(page):
    """Regex-flag columns are appended to the picker as Yes/No multiselects."""
    _open_column_picker(page)
    opts = page.query_selector_all(".filter-modal .filter-option")
    # More options than the base set means config flag columns were appended.
    assert len(opts) > len(BASE_FILTER_COLUMNS), "No flag filter columns present"
    opts[-1].click()  # last option is a config flag column
    page.wait_for_selector(".filter-modal .filter-options .filter-option input[type='checkbox']")
    labels = [o.inner_text().strip() for o in page.query_selector_all(".filter-modal .filter-options .filter-option")]
    assert any(l.startswith("Yes") for l in labels), f"Flag filter not Yes/No: {labels}"
    assert any(l.startswith("No") for l in labels), f"Flag filter not Yes/No: {labels}"
    page.query_selector(".filter-modal .filter-options input[type='checkbox']").click()
    page.click(".filter-modal .btn-apply")
    chips = page.query_selector_all(".filter-chip")
    assert len(chips) > 0, "Applying a flag filter created no chip"


def test_select_all_checks_all_options(page):
    """The 'Select all' checkbox checks every visible option."""
    _open_multiselect_filter(page, "State")
    page.query_selector(".filter-modal .selectall-cb").click()
    boxes = page.query_selector_all(".filter-modal .filter-options input[type='checkbox']")
    assert len(boxes) > 0
    assert all(b.is_checked() for b in boxes), "Select all did not check every option"


def test_option_search_filters_list(page):
    """Typing in the per-column search box narrows the option list."""
    _open_multiselect_filter(page, "State")
    initial = len(page.query_selector_all(".filter-modal .filter-options .filter-option:visible"))
    page.fill(".filter-modal .filter-search", "CA")
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
    _open_multiselect_filter(page, "State")
    search = page.query_selector(".filter-modal .filter-search")
    assert search.is_visible(), "Search should be visible for the State filter"


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

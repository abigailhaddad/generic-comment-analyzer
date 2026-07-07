"""Tests for the 'Read the Rule' page (read-the-rule.html).

Only runs when a rule page has been built at the project root (OMB-style
regulation with rule_sections.json). Skipped otherwise (a regulation without rule_sections.json).
"""

import os
import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RULE_FILE = os.path.join(ROOT_DIR, "read-the-rule.html")

pytestmark = pytest.mark.skipif(
    not os.path.exists(RULE_FILE), reason="read-the-rule.html not built (regulation has no rule_sections.json)"
)


@pytest.fixture
def rule_page(server, browser_ctx):
    pg = browser_ctx.new_page()
    pg.goto(f"{server}/read-the-rule.html", wait_until="networkidle", timeout=30000)
    pg.wait_for_selector(".rule-section", timeout=10000)
    yield pg
    pg.close()


def test_rule_sections_render(rule_page):
    """The page renders proposed-rule sections."""
    sections = rule_page.query_selector_all(".rule-section")
    assert len(sections) > 0, "No rule sections rendered"


def test_section_stance_bars_colored(rule_page):
    """Section citation bars are stacked oppose(red)/support(green) — matching the report."""
    seg_o = rule_page.query_selector(".cite-details .seg-oppose")
    seg_s = rule_page.query_selector(".cite-details .seg-support")
    assert seg_o is not None and seg_s is not None, "No stance segments on section bars"
    o_color = rule_page.evaluate("e => getComputedStyle(e).backgroundColor", seg_o)
    s_color = rule_page.evaluate("e => getComputedStyle(e).backgroundColor", seg_s)
    assert o_color == "rgb(192, 57, 43)", f"Oppose not red: {o_color}"
    assert s_color == "rgb(45, 106, 79)", f"Support not green: {s_color}"


def test_samples_collapsed_by_default(rule_page):
    """Citing-comment samples are collapsed by default (no <details open>)."""
    assert len(rule_page.query_selector_all(".cite-details")) > 0
    assert len(rule_page.query_selector_all(".cite-details[open]")) == 0, "Samples should start collapsed"


def test_click_reveals_sample_and_link(rule_page):
    """Clicking a citation bar reveals a sample of citing comments + a view-all link."""
    rule_page.query_selector(".cite-details > summary").click()
    rule_page.wait_for_timeout(200)
    assert rule_page.query_selector(".cite-details[open]") is not None, "Click did not reveal citing comments"
    assert len(rule_page.query_selector_all(".cite-details[open] .sample-list li")) > 0, "No sample comments shown"
    link = rule_page.query_selector(".cite-details[open] .view-all")
    assert link is not None
    assert "sections_referenced=200." in link.get_attribute("href"), "View-all link missing section filter param"


def test_sample_rows_stance_colored(rule_page):
    """Sample rows carry a stance class (pos-oppose / pos-support / pos-unclear)."""
    rule_page.query_selector(".cite-details > summary").click()
    rule_page.wait_for_timeout(200)
    rows = rule_page.query_selector_all(".cite-details[open] .sample-list li")
    assert len(rows) > 0
    assert all(any(c in (r.get_attribute("class") or "") for c in ("pos-oppose", "pos-support", "pos-unclear")) for r in rows)


def test_jump_nav_and_sort_toggle(rule_page):
    """The jump-nav lists section anchors and the sort toggle reorders sections."""
    nav_links = rule_page.query_selector_all("#jumpNav .toc-link")
    assert len(nav_links) > 0, "No jump-nav links"
    assert nav_links[0].get_attribute("href").startswith("#sec-"), "Jump-nav link is not an anchor"

    def first_section_count():
        el = rule_page.query_selector("#sectionsContainer .rule-section")
        return int(el.get_attribute("data-count"))

    reading_first = first_section_count()
    # Switch to "Most discussed" — the first card should now be the most-cited.
    rule_page.query_selector(".sort-btn[data-sort='count']").click()
    rule_page.wait_for_timeout(200)
    counts = [int(el.get_attribute("data-count")) for el in rule_page.query_selector_all("#sectionsContainer .rule-section")]
    assert counts == sorted(counts, reverse=True), "Sections not sorted by count desc"
    assert counts[0] == max(counts)
    # Back to reading order
    rule_page.query_selector(".sort-btn[data-sort='reading']").click()
    rule_page.wait_for_timeout(200)
    orders = [int(el.get_attribute("data-order")) for el in rule_page.query_selector_all("#sectionsContainer .rule-section")]
    assert orders == sorted(orders), "Reading order not restored"


def test_view_all_links_carry_section_param(rule_page):
    """Every 'view all' link opens the analysis pre-filtered to that section."""
    links = rule_page.query_selector_all(".view-all")
    assert len(links) > 0
    hrefs = [l.get_attribute("href") for l in links]
    assert all("sections_referenced=200." in h for h in hrefs), "Some view-all links missing the section param"


def test_back_to_analysis_link(rule_page):
    """There is a link back to the comment analysis."""
    link = rule_page.query_selector(".nav-link")
    assert link is not None
    assert link.get_attribute("href").endswith(".html")


def test_other_sections_listed(rule_page):
    """Cited sections not amended by the rule are listed separately."""
    rows = rule_page.query_selector_all(".other-row")
    assert len(rows) > 0, "No 'other sections referenced' rows"

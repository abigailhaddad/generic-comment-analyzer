#!/usr/bin/env python3
"""Fetch a regulation's proposed-rule text from the Federal Register and parse it
into per-section JSON for the "Read the Rule" page.

Config-driven: reads the regulation's analyzer_config.yaml `rule_text` section, e.g.

    rule_text:
      federal_register_document: "2026-10817"   # FR document number
      part: "200"                               # CFR part to keep (sections §<part>.NNN)

Writes rule_sections.json in the regulation directory:
    [{number, sectno, heading, amendment, text}, ...]

Usage:
    python fetch_rule_text.py --regulation omb-financial-assistance
"""
import argparse
import json
import os
import re
import urllib.request
import xml.etree.ElementTree as ET

import yaml

FR_XML_URL = "https://www.federalregister.gov/api/v1/documents/{doc}.json?fields[]=full_text_xml_url"


def _text(el) -> str:
    return re.sub(r"\s+", " ", "".join(el.itertext())).strip()


def parse_sections(xml_bytes: bytes, part: str):
    """Parse FR proposed-rule XML into sections for the given CFR part."""
    root = ET.fromstring(xml_bytes)
    sections = []
    last_amendment = None
    for el in root.iter():
        if el.tag == "AMDPAR":
            last_amendment = _text(el)
        elif el.tag == "SECTION":
            sectno_el = el.find("SECTNO")
            subject_el = el.find("SUBJECT")
            sectno = _text(sectno_el) if sectno_el is not None else ""
            m = re.search(r"(\d+)\.(\d+)", sectno)
            if not m:
                continue
            number = f"{m.group(1)}.{m.group(2)}"
            paras = [_text(p) for p in el.findall(".//P")]
            sections.append({
                "number": number,
                "sectno": sectno,
                "heading": _text(subject_el) if subject_el is not None else "",
                "amendment": last_amendment or "",
                "text": "\n\n".join(paras),
            })
            last_amendment = None
    if part:
        sections = [s for s in sections if s["number"].startswith(f"{part}.")]
    return sections


def main():
    parser = argparse.ArgumentParser(description="Fetch + parse proposed-rule text by section")
    parser.add_argument("--regulation", type=str, help="Regulation slug under regulations/<slug>/")
    args = parser.parse_args()

    if args.regulation:
        reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "regulations", args.regulation)
        if not os.path.isdir(reg_dir):
            raise SystemExit(f"Regulation directory not found: {reg_dir}")
        os.chdir(reg_dir)

    with open("analyzer_config.yaml") as f:
        cfg = (yaml.safe_load(f) or {}).get("rule_text") or {}
    doc = cfg.get("federal_register_document")
    part = str(cfg.get("part", ""))
    if not doc:
        raise SystemExit("No rule_text.federal_register_document in analyzer_config.yaml")

    meta = json.loads(urllib.request.urlopen(FR_XML_URL.format(doc=doc)).read())
    xml_url = meta["full_text_xml_url"]
    xml_bytes = urllib.request.urlopen(xml_url).read()
    sections = parse_sections(xml_bytes, part)

    with open("rule_sections.json", "w", encoding="utf-8") as f:
        json.dump(sections, f, indent=1)
    print(f"Wrote rule_sections.json — {len(sections)} sections (part {part}) from FR {doc}")


if __name__ == "__main__":
    main()

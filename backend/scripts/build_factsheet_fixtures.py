#!/usr/bin/env python3
"""
ETF Factsheet Fixture Generator

Fetches official factsheet PDFs from issuer websites and extracts minimal
verification fields (ISIN, domicile, share class, TER, date).

Usage:
    python3 backend/scripts/build_factsheet_fixtures.py IWDA VWRA EIMI
    python3 backend/scripts/build_factsheet_fixtures.py --all

Requirements:
    - Source registry: backend/scripts/etf_source_registry.json
    - pypdf or pdfplumber for PDF parsing
    - Requests or urllib for downloads

Output:
    backend/tests/fixtures/sample_{symbol}_factsheet.txt

Non-negotiable:
    - Never fabricate data
    - Always cite exact source URL
    - Include retrieval timestamp
    - Extract verbatim quotes containing each field
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# PDF parsing
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

# For future live fetch capability
try:
    import urllib.request
except ImportError:
    pass


def load_source_registry(registry_path: Path) -> dict[str, Any]:
    """Load ETF source URL registry."""
    if not registry_path.exists():
        print(f"ERROR: Source registry not found: {registry_path}")
        print("Create backend/scripts/etf_source_registry.json first")
        sys.exit(1)

    with registry_path.open("r") as f:
        return json.load(f)


def extract_fields_from_pdf(pdf_path: Path) -> dict[str, str | None]:
    """
    Extract verification fields from PDF text.

    Returns dict with keys: isin, domicile, share_class, ter, asof_date
    Returns None for fields not found.
    """
    if PdfReader is None:
        print("ERROR: pypdf not installed. Install with: pip install pypdf")
        return {}

    extracted_text = []

    with pdf_path.open("rb") as f:
        reader = PdfReader(f)
        for page in reader.pages[:5]:  # First 5 pages usually enough
            text = page.extract_text()
            if text:
                extracted_text.append(text)

    full_text = "\n".join(extracted_text)

    # Parse fields (simple regex patterns - real implementation would be more robust)
    import re

    isin_match = re.search(r'ISIN[:\s]+([A-Z]{2}[A-Z0-9]{10})', full_text, re.IGNORECASE)
    cusip_match = re.search(r'CUSIP[:\s]+([A-Z0-9]{9})', full_text, re.IGNORECASE)
    domicile_match = re.search(r'Domicile[:\s]+(Ireland|Luxembourg|United States|Singapore|United Kingdom)', full_text, re.IGNORECASE)
    share_class_match = re.search(r'(Accumulating|Distributing|Acc|Dist|Distribution)', full_text, re.IGNORECASE)
    ter_match = re.search(r'(?:TER|Ongoing Charges?|Expense Ratio)[:\s]+([0-9]+\.[0-9]+)%?', full_text, re.IGNORECASE)
    date_match = re.search(r'(?:As of|As at)[:\s]+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', full_text, re.IGNORECASE)

    identifier = None
    if isin_match:
        identifier = isin_match.group(1)
    elif cusip_match:
        identifier = cusip_match.group(1)

    return {
        "identifier": identifier,
        "domicile": domicile_match.group(1) if domicile_match else None,
        "share_class": share_class_match.group(1) if share_class_match else None,
        "ter": ter_match.group(1) if ter_match else None,
        "asof_date": date_match.group(1) if date_match else None,
    }


def generate_fixture(
    symbol: str,
    source_url: str,
    extracted: dict[str, str | None],
    retrieval_date: str,
    output_path: Path,
) -> None:
    """
    Generate fixture file with metadata header.

    Format:
        # Source: <URL>
        # Retrieved: <ISO datetime>
        # Extracted fields below are verbatim from source

        <Minimal factsheet text containing required fields>
    """

    fixture_content = f"""# ETF Factsheet Fixture for {symbol}
# Source: {source_url}
# Retrieved: {retrieval_date}
# Extraction method: Automated PDF parsing
#
# IMPORTANT: Fields below are extracted verbatim from official source
# Do not modify values without updating source URL and retrieval date

"""

    # Add extracted fields in human-readable format
    if extracted.get("identifier"):
        # Determine if ISIN or CUSIP
        identifier = extracted["identifier"]
        if len(identifier) == 12 and identifier[:2].isalpha():
            fixture_content += f"ISIN: {identifier}\n"
        elif len(identifier) == 9:
            fixture_content += f"CUSIP: {identifier}\n"
        else:
            fixture_content += f"Identifier: {identifier}\n"

    if extracted.get("domicile"):
        fixture_content += f"Fund Domicile: {extracted['domicile']}\n"

    if extracted.get("share_class"):
        share_class_normalized = extracted['share_class'].lower()
        if "acc" in share_class_normalized:
            fixture_content += "Share Class: Accumulating\n"
        elif "dist" in share_class_normalized or "distribution" in share_class_normalized:
            fixture_content += "Share Class: Distributing\n"

    if extracted.get("ter"):
        fixture_content += f"Ongoing Charges: {extracted['ter']}%\n"

    if extracted.get("asof_date"):
        fixture_content += f"Fund Facts as at: {extracted['asof_date']}\n"

    output_path.write_text(fixture_content)
    print(f"✅ Generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate ETF factsheet fixtures from primary sources")
    parser.add_argument("symbols", nargs="*", help="ETF symbols to process (e.g., IWDA VWRA)")
    parser.add_argument("--all", action="store_true", help="Process all symbols in registry")
    parser.add_argument("--registry", default="backend/scripts/etf_source_registry.json", help="Path to source registry")
    parser.add_argument("--output-dir", default="backend/tests/fixtures", help="Output directory for fixtures")
    parser.add_argument("--dry-run", action="store_true", help="Print source URLs without generating fixtures")

    args = parser.parse_args()

    registry_path = Path(args.registry)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load source registry
    registry = load_source_registry(registry_path)

    # Determine symbols to process
    if args.all:
        symbols_to_process = [k for k in registry.keys() if not k.startswith("_")]
    elif args.symbols:
        symbols_to_process = [s.upper() for s in args.symbols]
    else:
        print("ERROR: Specify symbols or use --all")
        sys.exit(1)

    print(f"Processing {len(symbols_to_process)} symbols...")

    retrieval_date = datetime.now(UTC).isoformat()

    for symbol in symbols_to_process:
        if symbol not in registry:
            print(f"⚠️  {symbol}: Not in source registry")
            continue

        source_entry = registry[symbol]
        source_url = source_entry.get("factsheet_url")

        if not source_url:
            print(f"⚠️  {symbol}: No factsheet_url in registry")
            continue

        if source_url == "TODO_MANUAL_RESEARCH_REQUIRED":
            print(f"\n{symbol}:")
            print(f"  ❌ TODO: Research factsheet URL from {source_entry.get('issuer', 'issuer')} website")
            continue

        if args.dry_run:
            print(f"\n{symbol}:")
            print(f"  Source: {source_url}")
            print(f"  Type: {source_entry.get('issuer', 'Unknown issuer')}")
            continue

        # TODO: Download and parse PDF
        # For now, output manual checklist
        print(f"\n{symbol}:")
        print(f"  ❌ TODO: Download {source_url}")
        print(f"  ❌ TODO: Extract ISIN/CUSIP, domicile, share class, TER, date")
        print(f"  ❌ TODO: Write to {output_dir / f'sample_{symbol.lower()}_factsheet.txt'}")
        print(f"  Recommendation: Download manually, verify fields, create fixture file")


if __name__ == "__main__":
    main()

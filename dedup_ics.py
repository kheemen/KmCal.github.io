#!/usr/bin/env python3
"""
ICS Duplicate Event Detector & Remover

Scans one or more .ics files for duplicate VEVENT entries and removes them.

Duplicate detection strategy (in priority order):
  1. Exact UID match  – Two events sharing the same UID within a file.
  2. Content-key match – Same SUMMARY + DTSTART + DTEND but different UIDs
     (catches re-imported / re-exported copies of the same event).

When a duplicate is found the *first* occurrence is kept and subsequent
duplicates are removed.  The cleaned file is written back in-place (a
backup with the `.bak` suffix is created first).

Usage:
    python3 dedup_ics.py [FILES...]

If no files are given, the script processes every *.ics file in the
current directory.
"""

from __future__ import annotations

import glob
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class VEvent:
    """Lightweight representation of a VEVENT block."""
    raw: str                       # full text including BEGIN/END markers
    uid: str = ""
    summary: str = ""
    dtstart: str = ""
    dtend: str = ""
    line_index: int = 0            # position in the original event list

    @property
    def content_key(self) -> Tuple[str, str, str]:
        """Key used for content-based dedup (SUMMARY + DTSTART + DTEND)."""
        return (self.summary.strip(), self.dtstart.strip(), self.dtend.strip())


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_VEVENT_RE = re.compile(r"(BEGIN:VEVENT\r?\n.*?END:VEVENT)", re.DOTALL)


def _extract_field(block: str, name: str) -> str:
    """Return the value of the first line starting with *name* (case-insensitive).

    Handles property parameters (e.g. ``DTSTART;VALUE=DATE:20210404``) by
    returning the full line so the parameter info is preserved in the key.
    """
    for line in block.splitlines():
        stripped = line.strip()
        if stripped.upper().startswith(name.upper() + ":") or stripped.upper().startswith(name.upper() + ";"):
            return stripped
    return ""


def parse_events(text: str) -> List[VEvent]:
    """Parse all VEVENT blocks out of an ICS file body."""
    events: List[VEvent] = []
    for idx, m in enumerate(_VEVENT_RE.finditer(text)):
        block = m.group(1)
        ev = VEvent(
            raw=block,
            uid=_extract_field(block, "UID").split(":", 1)[-1] if _extract_field(block, "UID") else "",
            summary=_extract_field(block, "SUMMARY").split(":", 1)[-1] if _extract_field(block, "SUMMARY") else "",
            dtstart=_extract_field(block, "DTSTART"),
            dtend=_extract_field(block, "DTEND"),
            line_index=idx,
        )
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# Dedup logic
# ---------------------------------------------------------------------------

@dataclass
class DedupResult:
    """Holds the outcome of deduplication for a single file."""
    filepath: str
    total_events: int = 0
    uid_dupes: List[VEvent] = field(default_factory=list)
    content_dupes: List[VEvent] = field(default_factory=list)

    @property
    def duplicates_found(self) -> int:
        return len(self.uid_dupes) + len(self.content_dupes)


def dedup_events(events: List[VEvent]) -> DedupResult:
    """Identify duplicate events.  Returns a DedupResult with lists of dupes."""
    result = DedupResult(filepath="", total_events=len(events))

    seen_uids: Dict[str, VEvent] = {}
    seen_content: Dict[Tuple[str, str, str], VEvent] = {}
    uid_dupe_set: set = set()
    content_dupe_set: set = set()

    for ev in events:
        # --- UID-based ---
        if ev.uid:
            if ev.uid in seen_uids:
                if ev.uid not in uid_dupe_set:
                    uid_dupe_set.add(ev.uid)
                result.uid_dupes.append(ev)
                continue  # skip content check for UID dupes
            seen_uids[ev.uid] = ev

        # --- Content-based ---
        key = ev.content_key
        if key != ("", "", ""):  # skip events with no identifying info
            if key in seen_content:
                if key not in content_dupe_set:
                    content_dupe_set.add(key)
                result.content_dupes.append(ev)
                continue
            seen_content[key] = ev

    return result


# ---------------------------------------------------------------------------
# File rewriting
# ---------------------------------------------------------------------------

def remove_dupes_from_text(text: str, dupes: List[VEvent]) -> str:
    """Remove the raw VEVENT blocks listed in *dupes* from *text*.

    Only one occurrence is removed per duplicate entry.  The three
    replacement attempts (\\n, \\r\\n, bare) are mutually exclusive so
    that identical blocks are not accidentally removed twice.
    """
    for ev in dupes:
        if ev.raw + "\n" in text:
            text = text.replace(ev.raw + "\n", "", 1)
        elif ev.raw + "\r\n" in text:
            text = text.replace(ev.raw + "\r\n", "", 1)
        elif ev.raw in text:
            text = text.replace(ev.raw, "", 1)
    return text


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(result: DedupResult) -> None:
    filepath = result.filepath
    print(f"\n{'=' * 60}")
    print(f"File: {filepath}")
    print(f"Total events: {result.total_events}")
    print(f"{'=' * 60}")

    if result.uid_dupes:
        print(f"\n  UID duplicates removed: {len(result.uid_dupes)}")
        for ev in result.uid_dupes:
            print(f"    - UID={ev.uid}  SUMMARY={ev.summary}  DTSTART={ev.dtstart}")
    else:
        print("\n  No UID duplicates found.")

    if result.content_dupes:
        print(f"\n  Content duplicates removed (same SUMMARY+DTSTART+DTEND): {len(result.content_dupes)}")
        for ev in result.content_dupes:
            print(f"    - UID={ev.uid}  SUMMARY={ev.summary}  DTSTART={ev.dtstart}")
    else:
        print("  No content duplicates found.")

    if result.duplicates_found == 0:
        print("\n  ✓ File is clean — no duplicates detected.")
    else:
        print(f"\n  Removed {result.duplicates_found} duplicate(s) total.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_file(filepath: str) -> DedupResult:
    """Process a single .ics file: detect dupes, back up, and rewrite."""
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    events = parse_events(text)
    result = dedup_events(events)
    result.filepath = filepath

    if result.duplicates_found > 0:
        all_dupes = result.uid_dupes + result.content_dupes
        # Create backup
        backup = filepath + ".bak"
        shutil.copy2(filepath, backup)
        print(f"  Backup saved to {backup}")

        cleaned = remove_dupes_from_text(text, all_dupes)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(cleaned)

    return result


def main() -> None:
    files = sys.argv[1:]
    if not files:
        files = sorted(glob.glob("*.ics"))

    if not files:
        print("No .ics files found.")
        sys.exit(0)

    print(f"Scanning {len(files)} file(s): {', '.join(files)}")

    total_dupes = 0
    for filepath in files:
        if not os.path.isfile(filepath):
            print(f"WARNING: {filepath} not found, skipping.")
            continue
        result = process_file(filepath)
        print_report(result)
        total_dupes += result.duplicates_found

    print(f"\n{'=' * 60}")
    if total_dupes == 0:
        print("All files are clean. No duplicates found.")
    else:
        print(f"Done. Removed {total_dupes} duplicate(s) across all files.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

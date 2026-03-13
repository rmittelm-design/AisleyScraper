from __future__ import annotations

import argparse

from aisley_scraper.config import get_settings
from aisley_scraper.storage_integrity import (
    delete_orphan_storage_objects,
    detect_orphan_storage_objects,
)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Find and optionally delete unlinked Supabase storage objects")
    p.add_argument("--apply", action="store_true", help="Delete orphaned objects (default: dry run)")
    p.add_argument("--batch-size", type=int, default=200, help="Delete batch size when --apply is used")
    return p


def main() -> int:
    args = _parser().parse_args()
    s = get_settings()

    audit = detect_orphan_storage_objects(s)
    orphan_paths = list(audit["orphan_paths"])

    print(
        {
            "linked_paths": audit["linked_paths"],
            "stored_paths": audit["stored_paths"],
            "orphan_paths": len(orphan_paths),
            "mode": "apply" if args.apply else "dry-run",
        }
    )

    if not orphan_paths or not args.apply:
        if orphan_paths:
            print({"sample_orphans": orphan_paths[:10]})
        return 0

    deleted = delete_orphan_storage_objects(s, orphan_paths, batch_size=args.batch_size)

    print({"deleted_orphans": deleted})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

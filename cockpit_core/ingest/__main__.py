"""CLI entry point: python -m cockpit_core.ingest [options]"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def _parse_date(s: str) -> date:
    if s.lower() in ("today", "now"):
        return date.today()
    return date.fromisoformat(s)


def cmd_run(args: argparse.Namespace) -> int:
    from cockpit_core.config import load_config
    from cockpit_core.ingest.runner import IngestRunner

    config = load_config()
    if args.project_key:
        config.project_key = args.project_key

    runner = IngestRunner(config)

    target_date = _parse_date(args.date)
    result = runner.run(target_date, force=args.force, lookback_days=args.lookback)

    if result.skipped:
        print(f"Skipped {target_date} (already exists). Use --force to re-ingest.")
        return 0
    if result.status == "ok":
        print(
            f"✓ {target_date} ingested: "
            f"{result.issues_seen} issues, "
            f"{result.worklogs_seen} worklogs, "
            f"{result.transitions_seen} transitions"
        )
        return 0
    print(f"✗ Ingestion failed: {result.error_message}", file=sys.stderr)
    return 1


def cmd_list(args: argparse.Namespace) -> int:
    from cockpit_core.config import load_config
    from cockpit_core.storage.repo import CockpitRepository

    config = load_config()
    repo = CockpitRepository(config.db_path)
    runs = repo.list_recent_runs(limit=args.limit)
    if not runs:
        print("No runs found.")
        return 0
    print(f"{'ID':>4}  {'Date':>12}  {'Project':>8}  {'Status':>8}  {'Issues':>7}  {'Worklogs':>9}  Started")
    print("-" * 78)
    for r in runs:
        print(
            f"{r['id']:>4}  {r['target_date']:>12}  {r['project_key']:>8}  "
            f"{r['status']:>8}  {r['issues_seen'] or 0:>7}  "
            f"{r['worklogs_seen'] or 0:>9}  {r['started_at'][:19]}"
        )
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    from cockpit_core.config import load_config
    from cockpit_core.ingest.backfill import BackfillRunner

    config = load_config()
    if args.project_key:
        config.project_key = args.project_key

    from_date = _parse_date(args.from_date)
    to_date   = _parse_date(args.to_date)
    runner = BackfillRunner(config)
    result = runner.run(from_date, to_date, force=args.force, progress=print)

    if result.status == "ok":
        print(
            f"✓ Backfill complete: {result.days_written} days written, "
            f"{result.days_skipped} skipped. "
            f"Issues: {result.issues_fetched}, Worklogs: {result.worklogs_fetched}, "
            f"Transitions: {result.transitions_fetched}"
        )
        return 0
    print(f"✗ Backfill failed: {result.error_message}", file=sys.stderr)
    return 1


def cmd_verify(args: argparse.Namespace) -> int:
    from cockpit_core.config import load_config
    from cockpit_core.ingest.runner import IngestRunner

    config = load_config()
    runner = IngestRunner(config)
    ok = runner.verify_credentials()
    if ok:
        print("✓ Jira credentials valid.")
        return 0
    print("✗ Credential verification failed.", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m cockpit_core.ingest")
    sub = parser.add_subparsers(dest="cmd")

    # run subcommand (also the default)
    run_p = sub.add_parser("run", help="Ingest a date")
    run_p.add_argument("--date", default="today", help="ISO date or 'today'")
    run_p.add_argument("--project-key", "--project_key", default="")
    run_p.add_argument("--force", action="store_true")
    run_p.add_argument("--lookback", type=int, default=7)

    sub.add_parser("verify", help="Verify Jira credentials")
    list_p = sub.add_parser("list-runs", help="List recent ingestion runs")
    list_p.add_argument("--limit", type=int, default=20)

    bf_p = sub.add_parser("backfill", help="Reconstruct historical snapshots from full Jira history")
    bf_p.add_argument("--from", dest="from_date", required=True, help="Start date (YYYY-MM-DD or 'today')")
    bf_p.add_argument("--to", dest="to_date", required=True, help="End date (YYYY-MM-DD or 'today')")
    bf_p.add_argument("--project-key", "--project_key", default="")
    bf_p.add_argument("--force", action="store_true", help="Overwrite existing snapshots")

    # Allow flat invocation: python -m cockpit_core.ingest --date today
    if len(sys.argv) > 1 and sys.argv[1].startswith("-"):
        sys.argv.insert(1, "run")

    args = parser.parse_args()
    if args.cmd == "run" or args.cmd is None:
        if not hasattr(args, "date"):
            parser.print_help()
            return 0
        return cmd_run(args)
    elif args.cmd == "verify":
        return cmd_verify(args)
    elif args.cmd == "list-runs":
        return cmd_list(args)
    elif args.cmd == "backfill":
        return cmd_backfill(args)
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())

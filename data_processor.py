import csv
import json
import sys
import argparse
from typing import List, Dict
from datetime import datetime

def load_json(path: str) -> List[Dict]:
    """
    Load records from a JSON file.
    Expected format: list of dictionaries.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found: {path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON file: {path}", file=sys.stderr)
        sys.exit(1)

    return data


def validate_records(records: List[Dict]) -> None:
    """
    Validate basic structure of records.
    Raises error and exits if validation fails.
    """
    if not isinstance(records, list):
        print("ERROR: JSON root must be a list", file=sys.stderr)
        sys.exit(1)

    required_keys = {"user", "event", "status"}

    for idx, record in enumerate(records):
        if not isinstance(record, dict):
            print(f"ERROR: Record {idx} is not an object", file=sys.stderr)
            sys.exit(1)

        missing = required_keys - record.keys()
        if missing:
            print(
                f"ERROR: Record {idx} missing keys: {missing}",
                file=sys.stderr
            )
            sys.exit(1)

def aggregate_records(records):
    """
    Aggregate records by user.
    Returns a dict keyed by user with counts and events sets.
    """
    summary = {
        "users": {},
        "by_hour": {}
    }

    for record in records:
        user= record["user"]
        status = record["status"]
        event = record["event"]

        if user not in summary["users"]:
            summary["users"][user] = {
                "total": 0,
                "success": 0,
                "fail": 0,
                "events": set()
            }

        summary["users"][user]["total"] += 1
        summary["users"][user]["events"].add(event)

        if status == "success":
            summary["users"][user]["success"] += 1
        else:
            summary["users"][user]["fail"] += 1

        by_hour = {}

        for r in records:
            ts = r.get("timestamp")
            if not ts:
                continue

            dt = datetime.fromisoformat(ts)
            hour_key = dt.strftime("%Y-%m-%d %H")

            if hour_key not in by_hour:
                by_hour[hour_key] = {
                    "total_events": 0,
                    "success": 0,
                    "fail": 0
                }

            by_hour[hour_key]["total_events"] += 1

            if r.get("status") == "fail":
                by_hour[hour_key]["fail"] += 1
            else:
                by_hour[hour_key]["success"] +=1

        summary["by_hour"] = by_hour

    return summary

def build_stats(output):
    users = output.get("users", [])

    total_users = len(users)
    total_events = sum(u["total_events"] for u in users)
    total_failures = sum(u["fail"] for u in users)
    users_with_failures = sum(1 for u in users if u["fail"] > 0)

    failure_rate = (
        (total_failures / total_events) * 100
        if total_events > 0 else 0
    )

    return {
        "total_users": total_users,
        "users_with_failures": users_with_failures,
        "total_events": total_events,
        "total_failures": total_failures,
        "failure_rate": round(failure_rate, 2),
    }

def build_output(summary):
    """
    Build final output structure from aggregated data.
    Converts internal sets and computes derived values.
    """
    users_out = []

    for user, data in summary["users"].items():
        total = data["total"]
        success = data["success"]
        fail = data["fail"]

        failure_rate = (
            (fail / total) * 100
            if total > 0 else 0
        )

        users_out.append({
            "user": user,
            "total_events": total,
            "success": success,
            "fail": fail,
            "failure_rate": round(failure_rate, 2),
            "event_types": sorted(data["events"]),
        })

    return{
        "users": users_out,
        "by_hour": summary.get("by_hour", {})
    }

def print_stats(stats):
    print("\nOverall statistics:")
    print("-" * 40)
    print(f"Total users: {stats['total_users']}")
    print(f"Users with failures: {stats['users_with_failures']}")
    print(f"Total events: {stats['total_events']}")
    print(f"Total failures: {stats['total_failures']}")
    print(f"Failure rate: {stats['failure_rate']:.1f}%")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process structured event data and generate summaries."
    )

    parser.add_argument(
        "input",
        help="Path to input JSON file"
    )

    parser.add_argument(
        "--json",
        dest="json_output",
        help="Write summary to JSON file"
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress console output."
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Do not print summary to console."
    )

    parser.add_argument(
        "--csv",
        dest="csv_output",
        help="Write summary to CSV file"
    )

    parser.add_argument(
        "--only-failures",
        action="store_true",
        help="Show only users with at least one failure"
    )

    parser.add_argument(
        "--min-failures",
        type=int,
        default=None,
        help="Show only users with at least N failures"
    )
    parser.add_argument(
        "--by-hour",
        action="store_true",
        help="Show failures by hour"
    )

    return parser.parse_args()

def filter_users(output, only_failures=False, min_failures=None):
    users = output.get("users",[])

    # Apply only failures
    if only_failures is not None:
        users = [u for u in users if u["fail"] > 0]

    # Apply min-failures
    if min_failures is not None:
        users = [u for u in users if u["fail"] >= min_failures]

    return {"users": users}

def print_summary(output):
    """
    Print a human-readable summary to the console.
    """
    users = output.get("users", [])

    if not users:
        print("No user data to display.")
        return

    print("\nSummary by user:")
    print("-" * 40)

    for u in users:
        print(f"User: {u['user']}")
        print(f"  Total events:     {u['total_events']}")
        print(f"  Success:          {u['success']}")
        print(f"  Fail:         {u['fail']}")
        print(f"  Failure rate:     {u['failure_rate'] * 100:.1f}%")
        print(f"  Event types:     {', '.join(u['event_types'])}")
        print("-" * 40)

def print_by_hour(by_hour):
    print("\nEvents by hour:")
    print("-" * 30)

    for hour, data in sorted(by_hour.items()):
        print(
            f"{hour}: "
            f"{data['total_events']} events "
            "({data['success']} success, {data['fail']} fail"
        )

def print_by_hour(by_hour):
    print("\nEvents by hour:")
    print("-" * 30)

    for hour, data in sorted(by_hour.items()):
        print(
            f"{hour}: "
            f"{data['total_events']} events "
            f"({data['success']} success, {data['fail']} fail)"
        )

def write_json(output, path):
    """
    Write processed output to a JSON file.
    """
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
    except OSError as e:
        print(f"ERROR: Failed to write JSON file: {e}", file=sys.stderr)
        sys.exit(1)

def write_csv(output, path):
    """
    Write processed output to a CSV file.
    """
    try:
        with open(path, "w", encoding="utf-8") as f:
            fieldnames = [
                "user",
                "total_events",
                "success",
                "fail",
                "failure_rate",
                "event_types",
            ]

            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                quoting=csv.QUOTE_ALL
            )

            writer.writeheader()

            for user_data in output.get("users", []):
                writer.writerow({
                    "user": user_data["user"],
                    "total_events": user_data["total_events"],
                    "success": user_data["success"],
                    "fail": user_data["fail"],
                    "failure_rate": user_data["failure_rate"] * 100,
                    "event_types": sorted(user_data["event_types"]),
                })

    except OSError as e:
        print(f"ERROR: Failed to write CSV file: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    # IMPORTANT
    # Stats MUST be built from post-filtered output.
    # Do not move build_stats() above filter_users().
    args = parse_args()

    records = load_json(args.input)
    validate_records(records)

    summary = aggregate_records(records)

    output = build_output(summary)

    if args.only_failures or (args.min_failures is not None and args.min_failures >0):
        output = filter_users(
            output,
            only_failures=args.only_failures,
            min_failures=args.min_failures,
    )

    stats = build_stats(output)

    assert stats["total_users"] == len(output["users"]), \
        "Invariant failed: total_users does not match output users"

    if not args.quiet and not args.no_summary:
        print_summary(output)
        print_stats(stats)

    if args.by_hour:
            print_by_hour(summary["by_hour"])

    if args.json_output:
        json_data = dict(output)

        if args.by_hour:
            json_data["by_hour"] = summary["by_hour"]

        write_json(output, args.json_output)

        if not args.quiet:
            print(f"\nJSON written to {args.json_output}")

    if args.csv_output:
        write_csv(output, args.csv_output)
        if not args.quiet:
            print(f"CSV written to {args.csv_output}")

if __name__ == "__main__":
    main()

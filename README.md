# Data Processor (V1.1)

## Overview

**Data Processor** is a command-line Python tool that ingests JSON event records,
aggregates them by user, and produces:
    * Per-user summaries
    * Overall statistics
    * Optional time-based (hourly) aggregation

The tool supports **console output**, **CSV export**, and **JSON export**, with behaviour controlled via CLI flags.

This project was developed as a learning exercise focused on:
    * Data aggregation pipelines
    * CLI design
    * Schema discipline
    * Defensive programming and invariants
---
## Key Features

* Aggregate events per user:
  * Total events
  * Successes and failures
  * Failure rate
  * Event types encountered
* Overall statistics across all users
* Filtering:
  * `--only-failures`
  * `--min-failures N`
* Time aggregation:
  * Hourly breakdown via `--by-hour`
* Multiple output formats:
  * Console (human-readable)
  * CSV (per-user rows)
  * JSON (structured export)

---
## Expected Input Schema
Each input file must be a JSON array of event records.

### **Required fields**

{
  "user": "alice",
  "status": "success" | "fail",
  "event": "login"
}

```
### **Optional fields**

{
  "timestamp": "YYYY-MM-DD HH:MM:SS"
}

```
> ⚠️ **Important**
> The `timestamp` field is required **only** when using `--by-hour`.
> Records without a timestamp are ignored for time aggregation.

---

## Internal Schema (Stable)

From V1.1 onward, the tool uses a **single, consistent internal schema**.

### Per-user aggregation:

user
total_events
success
fail
failure_rate
event_types

### Time aggregation (`--by-hour`):

by_hour:
  YYYY-MM-DD HH:
    total_events
    success
    fail

> Earlier development versions experimented with alternate field names
> (`total`, `events`, `successes`, `failures`).
> These are **not supported** and will cause runtime errors if reintroduced.

---
## Console Output Behaviour

Console output is designed to provide **immediate visibility** into per-user results and overall statistics,
without overwhelming the user.

### Default behaviour (no flags)

* Prints **per-user summaries**
* Prints **overall statistics**
* Does **not** display time-based (hourly) aggregation

### With `--by-hour`

* Adds an **hourly breakdown** after the per-user and overall statistics

### With `--quiet`

* Suppresses all console output

> Note: Console output is intended for **human review**.
> CSV and JSON outputs remain the authoritative machine-readable formats.

---

## CSV Output Behaviour

* Always contains **per-user rows only**
* One row per user
* No overall statistics
* No time aggregation

This makes CSV suitable for spreadsheets and further analysis.

---

## JSON Output Behaviour

JSON output is the **most complete and flexible export format**.

* Always includes per-user data
* Always includes `by_hour` aggregation **when timestamps are present**
* The `--by-hour` flag primarily affects **console visibility**, not JSON structure
* Respects filtering flags:

  * `--only-failures`
  * `--min-failures`

Example structure:

{
  "users": [...],
  "by_hour": {...}
}

This makes JSON suitable for:
    * Further automated processing
    * Dashboards
    * Downstream analytics

---

## CLI Options

--only-failures        Include only users with failures
--min-failures N       Include users with at least N failures
--by-hour              Enable hourly time aggregation
--json OUTPUT.json     Write JSON output
--csv OUTPUT.csv       Write CSV output
--quiet                Suppress console output
--no-summary           Suppress console summary

---

## Version History

### V1.0

* Core per-user aggregation
* Console, CSV, and JSON output
* Failure-based filtering
* Initial statistics reporting

### V1.1 (Current Stable)

* Added hourly time aggregation (`--by-hour`)
* Introduced pipeline invariant checks
* Clarified and stabilized internal schema
* Resolved schema crossover issues discovered during development
* Improved separation of concerns between console, CSV, and JSON outputs

---

## Design Notes & Known Pitfalls

* The tool relies on **strict schema consistency** across the pipeline.
  Mixing field names (e.g. `total` vs `total_events`) will break execution.
* Time-based aggregation (`by_hour`) is **always calculated** when timestamps are available.
* The `--by-hour` flag controls **console display**, not whether aggregation occurs.
* CSV output intentionally excludes time aggregation to preserve flat, row-based structure.
* Console output prioritizes readability; CSV and JSON are authoritative exports.
* Invariant checks are used to catch silent pipeline corruption early.

---

## Status

**V1.1 is stable and verified.**
All CLI combinations, console output, CSV export, and JSON export behave as intended.

Darren Williamson
Python Utility Development * Automation * Data Analysis
Uk Citizen / Spain-based / Remote
LinkedIn: https://www.linkedin.com/in/darren-williamson3/

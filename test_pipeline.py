from data_processor_v1 import (
    load_json,
    aggregate_records,
    build_output,
    filter_users,
    build_stats,
)

def test_stats_match_filtered_output():
    # Arranga
    records = load_json("test_data.json")

    # Act (pipeline order)
    summary = aggregate_records(records)
    output = build_output(summary)
    filtered = filter_users(output, only_failures=True)
    stats = build_stats(filtered)

    # Assert (the invariant)
    assert stats["total_users"] == len(filtered["users"])
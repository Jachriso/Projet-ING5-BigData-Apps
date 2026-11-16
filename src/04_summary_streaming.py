# 04_summary_streaming.py
# Turns the raw Spark console output of 04_streaming.py into two clean tables :
#   1. all the matching events (empty batches are skipped)
#   2. the last state of each (window, kind) count
#
# Run : python src/04_summarize_streaming.py reports/04_streaming.txt
# Run and save in report file : python src/04_summary_streaming.py reports/04_streaming.txt > reports/04_summary_streaming.txt

import sys


def parse_row(line):
    """Split a console table line '|a|b|c|' into ['a', 'b', 'c']."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def print_table(header, rows):
    """Print rows as aligned columns."""
    widths = [max(len(str(x)) for x in col) for col in zip(header, *rows)]
    for row in [header] + rows:
        print("  ".join(str(x).ljust(w) for x, w in zip(row, widths)).rstrip())


def main(path):
    events, windows = [], {}
    event_header = window_header = None
    current = None  # which table we are reading: "events" or "windows"

    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.startswith("|"):
                continue
            row = parse_row(line)
            if row[0] == "event_time":
                event_header, current = row, "events"
            elif row[0] == "window":
                window_header, current = row, "windows"
            elif current == "events":
                events.append(row)
            elif current == "windows":
                # update mode: a later row replaces the previous count
                windows[(row[0], row[1])] = row

    print(f"Matching events: {len(events)}")
    if events:
        print_table(event_header, events)

    print("\nLast state of each window:")
    if windows:
        print_table(window_header, list(windows.values()))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "reports/04_streaming.txt")

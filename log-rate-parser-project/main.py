from collections import deque
from datetime import datetime
import sys
import pathlib


def line_generator(filename):
    with open(filename, 'r') as file:
        for line in file:
            yield line.strip()

def convert_to_timestamp(line):
    dt = datetime.strptime(line, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())

def convert_to_date_time(timestamp):
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def calculate_events_per_minute(timestamps):
    total_seconds = timestamps[-1] - timestamps[0]
    return len(timestamps) / (total_seconds / 60)

def calculate_busiest_5_minute_window(timestamps):
    window = deque()
    max_window_start = 0
    max_window_stop = 0
    max_events = 0

    for timestamp in timestamps:
        window.append(timestamp)
        while timestamp - window[0] > 300:
            window.popleft()

        if len(window) > max_events:
            max_window_start = window[0]
            max_window_stop = window[-1]
            max_events = len(window)

    return convert_to_date_time(max_window_start), convert_to_date_time(max_window_stop), max_events

def calculate_gaps_in_data(timestamps, gap_mins):
    gap_seconds = gap_mins * 60

    for index in range(1, len(timestamps)):
        if timestamps[index] - timestamps[index - 1] > gap_seconds:
            start_time = convert_to_date_time(timestamps[index - 1])
            stop_time = convert_to_date_time(timestamps[index])
            return start_time, stop_time
        
    return None, None

def main(filename):
    timestamps = []
    for line in line_generator(filename):
        timestamp_text = " ".join(line.split(" ")[:2])
        timestamp = convert_to_timestamp(timestamp_text)
        timestamps.append(timestamp)

    events_per_minute = calculate_events_per_minute(timestamps)
    print(f"Events per Minute: {events_per_minute:.2f}")

    start_time, stop_time, num_events = calculate_busiest_5_minute_window(timestamps)
    print(f"Busiest 5-Minute Window: {start_time} to {stop_time} with {num_events} events")

    gap_mins = 2
    start_time, stop_time = calculate_gaps_in_data(timestamps, gap_mins)
    if start_time and stop_time:
        print(f"Gaps in Data greater than {gap_mins} minutes: {start_time} to {stop_time}")


if __name__ == "__main__":
    filename = sys.argv[1]
    if not pathlib.Path(filename).is_file():
        print(f"Error: {filename} does not exist.")
        sys.exit(1)

    main(filename)

import csv
from datetime import datetime
from collections import defaultdict

input_file = "Nigeria.csv"

output_file = "sorted_dates_per_location.txt"

location_dates = defaultdict(list)

with open(input_file, "r") as f:
    reader = csv.reader(f)
    for row in reader:
        try:
            event_date, location, event_type = row[1], row[0], row[3]
            
            if "regains" in event_type.lower():
                formatted_date = datetime.strptime(event_date, "%m/%d/%y").strftime("%Y-%m-%d")
                
                location_dates[location].append(formatted_date)
        except (IndexError, ValueError):
            continue

with open(output_file, "w") as f:
    for location, dates in location_dates.items():
        f.write(f"{location}\t{', '.join(sorted(dates))}\n")

print(f"Results written to {output_file}")


#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

for line in sys.stdin:
    reader = csv.reader([line.strip()])
    for row in reader:
        try:
            event_date, location, event_type = row[0], row[1], row[2]
            
            if "regains" in event_type.lower():
                formatted_date = datetime.strptime(event_date, "%m/%d/%y").strftime("%Y-%m-%d")
             
                print(f"{location}\t{formatted_date}")
        except (IndexError, ValueError):
            continue


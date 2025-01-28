#!/usr/bin/env python3
import sys

current_location = None
dates = []

for line in sys.stdin:
    line = line.strip()
    location, date = line.split("\t")
    
    if current_location == location:
        dates.append(date)
    else:
        if current_location is not None:
            print(f"{current_location}\t{', '.join(sorted(dates))}")
        
        current_location = location
        dates = [date]

if current_location is not None:
    print(f"{current_location}\t{', '.join(sorted(dates))}")


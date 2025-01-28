#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    fields = line.split()
    
    if len(fields) < 5:
        continue
    
    store = fields[2]
    try:
        cost = float(fields[4])
    except ValueError:
        continue 
    
    print(f"{store}\t{cost}")


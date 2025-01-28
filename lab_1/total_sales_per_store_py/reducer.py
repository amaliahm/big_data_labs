#!/usr/bin/env python3
import sys

current_store = None
current_total = 0.0

for line in sys.stdin:
    line = line.strip()
    store, cost = line.split("\t")
    
    try:
        cost = float(cost)
    except ValueError:
        continue  
    
    if current_store == store:
        current_total += cost
    else:
        if current_store is not None:
            print(f"{current_store}\t{current_total}")
        
        current_store = store
        current_total = cost

if current_store is not None:
    print(f"{current_store}\t{current_total}")


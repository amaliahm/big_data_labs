#!/usr/bin/env python3
import sys

for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        sorted_key = ''.join(sorted(word))
        print(f"{sorted_key}\t{word}")


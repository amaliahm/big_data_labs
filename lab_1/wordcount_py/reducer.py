#!/usr/bin/env python
import sys

word_count = {}

for line in sys.stdin:
    line = line.strip()
    word, count = line.split("\t", 1)
    count = int(count)
    
    if word in word_count:
        word_count[word] += count
    else:
        word_count[word] = count

for word in word_count:
    print(f"{word}\t{word_count[word]}")


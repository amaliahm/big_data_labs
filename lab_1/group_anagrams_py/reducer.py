#!/usr/bin/env python3
import sys
from collections import defaultdict

anagrams = defaultdict(list)

for line in sys.stdin:
    sorted_key, word = line.strip().split("\t")
    anagrams[sorted_key].append(word)

for key, words in anagrams.items():
    print(f"{key}\t{words}")


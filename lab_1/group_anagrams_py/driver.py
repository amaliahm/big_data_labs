# Mapper
def mapper(input_file, intermediate_file):
    with open(input_file, "r") as infile, open(intermediate_file, "w") as outfile:
        for line in infile:
            words = line.strip().split()
            for word in words:
                sorted_key = ''.join(sorted(word))
                outfile.write(f"{sorted_key}\t{word}\n")

def reducer(intermediate_file, output_file):
    from collections import defaultdict

    anagrams = defaultdict(list)

    with open(intermediate_file, "r") as infile:
        for line in infile:
            sorted_key, word = line.strip().split("\t")
            anagrams[sorted_key].append(word)

    with open(output_file, "w") as outfile:
        for key, words in anagrams.items():
            outfile.write(f"{key}\t{words}\n")

input_file = "input_Anagram.txt"
intermediate_file = "intermediate.txt"
output_file = "output_Anagram.txt"

mapper(input_file, intermediate_file)
reducer(intermediate_file, output_file)

print(f"Anagrams grouped and written to {output_file}")


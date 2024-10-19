#! /usr/bin/env python3

'''Check for tags in a model that are not in the ontology.'''

from pathlib import Path
import sys

def main():
    '''Check for tags in a model that are not in the ontology.'''
    if len(sys.argv) != 3:
        sys.exit("Usage: check_brick_tags.py <ontology_ttl> <model_ttl")

    ontology_file = Path(sys.argv[1])
    if not ontology_file.exists():
        sys.exit(f"Error: {ontology_file} does not exist.")

    model_file = Path(sys.argv[2])
    if not model_file.exists():
        sys.exit(f"Error: {model_file} does not exist.")

    ontology_tags = set(get_brick_tags(ontology_file))
    model_tags = get_brick_tags(model_file)

    unrecognised_tags = [tag for tag in model_tags if tag not in ontology_tags]

    if unrecognised_tags:
        num_unrecognised_tags = len(unrecognised_tags)
        unrecognised_tags = set(unrecognised_tags)
        num_distinct_tags = len(unrecognised_tags)
        print(f'Found {num_distinct_tags} distinct tags across {num_unrecognised_tags} instances:')
        for tag in sorted(unrecognised_tags):
            print(tag)

def get_brick_tags(file):
    '''Return a list of all tags in the file that start with "brick:".'''
    tags = []
    with file.open() as f:
        for line in f:
            words = line.split()
            for word in words:
                if word.startswith("brick:"):
                    tags.append(word)
    return tags

if __name__ == "__main__":
    main()

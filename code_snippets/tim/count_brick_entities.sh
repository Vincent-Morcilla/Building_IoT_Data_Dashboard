#! /usr/bin/env dash

# Count the number of brick entities in the given file

if [ "$#" -ne 1 ]; then
    cmdname=$(basename "$0")
    echo "Usage: $cmdname <file>"
    exit 1
fi

file="$1"

num_entities=$(grep -c ' a brick:' "$file")
# num_entities=$(grep -E '^[^ ]+ a brick:\w+' "$file" | wc -l)

echo "Total number of brick entities: $num_entities"

grep -E 'a brick:' "$file" | sed -E 's/[^ ]+ a (brick:[^ ]+) ;/\1/' | sort | uniq -c 
# grep -E '^[^ ]+ a (brick:\w+)' "$file" | sed -E 's/[^ ]+ a (brick:[^ ]+) ;/\1/' | sort | uniq -c 

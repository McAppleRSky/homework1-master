#!/bin/sh

for dir in "An Imitation of Spenser" "Auguries of Innocence" "Fair Elanor"; do
  echo "Checking $dir folder"

  mkdir -p "results/$dir"
  echo "integration-test/$dir/text.txt"
  java -jar build/libs/homework2.jar \
    "integration-test/$dir/text.txt" \
    "results/$dir/result.txt"

  if ! diff --ignore-all-space \
    --ignore-case \
    "integration-test/$dir/result.txt" \
    "results/$dir/result.txt"; then
    exit 1
  fi
  echo "Successfully processed $dir folder"
done

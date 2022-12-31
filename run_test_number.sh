#!/bin/bash
set -e

FOLDER="$1"
NUMBER="$2"
if [ -n "$FOLDER" ] && [ -d "lang/interpret_test/$FOLDER" ] && [ "$NUMBER" -gt -1 ]; then
  go test -count=1 github.com/purpleidea/mgmt/lang/ -run "$FOLDER/test_#$NUMBER_" -v
else
  echo "usage:"
  echo "  $0 <folder> <number>"
  echo
  echo "Run a specific test. To list all the numbers:"
  echo "  list_test_numbers.sh <folder>"
fi

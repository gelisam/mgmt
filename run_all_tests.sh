#!/bin/bash
set -e

FOLDER="$1"
if [ -n "$FOLDER" ] && [ -d "lang/interpret_test/$FOLDER" ]; then
  go test -count=1 github.com/purpleidea/mgmt/lang/ -run "$FOLDER" -v
else
  echo "usage:"
  echo "  $0 <folder>"
  echo
  echo "Run a series of tests."
fi

#!/bin/bash
set -e

FOLDER="$1"
if [ -n "$FOLDER" ] && [ -d "lang/interpret_test/$FOLDER" ]; then
  go test -count=1 github.com/purpleidea/mgmt/lang/ -run "$FOLDER/" -short -v
else
  echo "usage:"
  echo "  $0 <folder>"
  echo
  echo "Map each file name in the folder to a test number, so you can run it as"
  echo "  run_test_number.sh <folder> <number>"
  echo
  echo "An example for <folder> would be \"TestAstFunc1\". To list all the folders:"
  echo "  ls lang/interpret_test/"
fi

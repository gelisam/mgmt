#!/bin/bash
set -e

./run_test.sh || true
fswatcher --throttle=100 --path lang -- ./run_test.sh

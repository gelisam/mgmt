#!/bin/bash
set -e

function mgmt() {
  go run -ldflags '-X main.program=mgmt -X main.version=0.0.22-18-g2f55423' main.go "$@"
}

function mcl() {
  mgmt run lang "$@"
}

#mcl examples/lang/exec_func.mcl
#mcl lang/interpret_test/TestAstFunc1/lambda-chained/main.mcl
#go test -count=1 github.com/purpleidea/mgmt/lang/ -run "TestChangingFunctionGraph0" -v
./run_test_number.sh TestAstFunc1 15
  # 2>&1 | grep lang/funcs/structs/function.go

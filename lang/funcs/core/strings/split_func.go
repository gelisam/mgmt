// Mgmt
// Copyright (C) 2013-2022+ James Shubin and the project contributors
// Written by James Shubin <james@shubin.ca> and the project contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package corestrings

import (
	"strings"

	"github.com/purpleidea/mgmt/lang/funcs/simple"
	"github.com/purpleidea/mgmt/lang/types"
)

func init() {
	simple.ModuleRegister(ModuleName, "split", &types.SimpleFn{
		T: types.NewType("func(a str, b str) []str"),
		V: Split,
	})
}

// Split splits the input string using the separator and returns the segments as
// a list.
func Split(input []types.Value) (types.Value, error) {
	str, sep := input[0].Str(), input[1].Str()

	segments := strings.Split(str, sep)

	listVal := types.NewList(types.NewType("[]str"))

	for _, segment := range segments {
		listVal.Add(&types.StrValue{
			V: segment,
		})
	}

	return listVal, nil
}

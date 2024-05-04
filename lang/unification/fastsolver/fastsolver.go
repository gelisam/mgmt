// Mgmt
// Copyright (C) 2013-2024+ James Shubin and the project contributors
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//
// Additional permission under GNU GPL version 3 section 7
//
// If you modify this program, or any covered work, by linking or combining it
// with embedded mcl code and modules (and that the embedded mcl code and
// modules which link with this program, contain a copy of their source code in
// the authoritative form) containing parts covered by the terms of any other
// license, the licensors of this program grant you additional permission to
// convey the resulting work. Furthermore, the licensors of this program grant
// the original author, James Shubin, additional permission to update this
// additional permission if he deems it necessary to achieve the goals of this
// additional permission.

package fastsolver

import (
	"context"
	"fmt"
	"strings"

	"github.com/purpleidea/mgmt/lang/interfaces"
	"github.com/purpleidea/mgmt/lang/types"
	"github.com/purpleidea/mgmt/lang/unification"
	unificationUtil "github.com/purpleidea/mgmt/lang/unification/util"
	"github.com/purpleidea/mgmt/util/errwrap"
)

// XXX GOTAGS='embedded_provisioner noaugeas novirt' make
// XXX ./mgmt run --tmp-prefix lang --only-unify test.mcl

const (
	// Name is the prefix for our solver log messages.
	Name = "fast"

	// OptimizationNotImplemented is the magic flag name to XXX.
	OptimizationNotImplemented = "not-implemented"

// // RecursionDepthLimit specifies the max depth that is allowed.
// // FIXME: RecursionDepthLimit is not currently implemented
// RecursionDepthLimit = 5 // TODO: pick a better value ?
)

func init() {
	unification.Register(Name, func() unification.Solver { return &FastInvariantSolver{} })
	unification.Register("", func() unification.Solver { return &FastInvariantSolver{} }) // DEFAULT XXX
}

// FastInvariantSolver is a fast invariant solver based on union find. It is
// intended to be computationally efficient.
type FastInvariantSolver struct {
	// Strategy is a series of methodologies to heuristically improve the
	// solver.
	Strategy map[string]string

	Debug bool
	Logf  func(format string, v ...interface{})

	// notImplemented tells the solver to XXX
	notImplemented bool
}

// Init contains some handles that are used to initialize the solver.
func (obj *FastInvariantSolver) Init(init *unification.Init) error {
	obj.Strategy = init.Strategy

	obj.Debug = init.Debug
	obj.Logf = init.Logf

	optimizations, exists := init.Strategy[unification.StrategyOptimizationsKey]
	if !exists {
		return nil
	}
	// TODO: use a query string parser instead?
	for _, x := range strings.Split(optimizations, ",") {
		if x == OptimizationNotImplemented {
			obj.notImplemented = true
			continue
		}
	}

	return nil
}

// Solve runs the invariant solver. It mutates the .Data field in the .Uni
// unification variables, so that each set contains a single type. If each of
// the sets contains a complete type that no longer contains any ?1 type fields,
// then we have succeeded to unify all of our invariants. If not, then our list
// of invariants must be ambiguous. This is O(N*K) where N is the number of
// invariants, and K is the size of the maximum type. Eg a list of list of map
// of int to str would be of size three. XXX OR IS IT FOUR?
func (obj *FastInvariantSolver) Solve(ctx context.Context, data *unification.Data) (*unification.InvariantSolution, error) {

	for _, x := range data.UnificationInvariants { // []*UnificationInvariant
		// TODO: Is this a good break point for ctx?
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// pass
		}

		//if x IS_AN_EXCLUSIVE_INVARIANT {
		// XXX: process that quadratically if necessary...
		//	continue
		//}

		// TODO: Should we pass ctx into Unify?
		if err := unificationUtil.Unify(x.Expect, x.Actual); err != nil {
			// Storing the Expr with this invariant is so that we
			// can generate this more helpful error message here.
			return nil, errwrap.Wrapf(err, "expr: %s", x.Expr) // XXX: is this message ok?
		}
	}

	// Now that all the constraints have been unified, we know everything there is
	// to know about every unification variable. Any remaining unification
	// variable is thus a place in the code where the type is ambiguous.
	var solutions []*interfaces.EqualsInvariant
	for _, x := range data.UnificationInvariants {
		zonkedType := zonk(x.Actual)
		if containsUnificationVariable(zonkedType) {
			return nil, fmt.Errorf("%s's type is ambiguous: %s", x.Expr, zonkedType)
		}
		solutions = append(solutions, &interfaces.EqualsInvariant{
			Expr: x.Expr,
			Type: zonkedType,
		})
	}

	return &unification.InvariantSolution{
		Solutions: solutions,
	}, nil
}

// zonk is a helper function that recursively replaces all unification variables
// with their actual types. "zonk" is the name which the GHC Haskell compiler
// uses for this transformation, whimsically claiming that zonk is named "after
// the sound it makes".
func zonk(typ *types.Type) *types.Type {
	switch typ.Kind {
	case types.KindUnification:
		return zonk(typ.Uni.Find().Data)
	case types.KindList:
		return &types.Type{Kind: types.KindList, Val: zonk(typ.Val)}
	case types.KindMap:
		return &types.Type{Kind: types.KindMap, Key: zonk(typ.Key), Val: zonk(typ.Val)}
	case types.KindStruct:
		m := make(map[string]*types.Type)
		for k, v := range typ.Map {
			m[k] = zonk(v)
		}
		return &types.Type{Kind: types.KindStruct, Map: m, Ord: typ.Ord}
	case types.KindFunc:
		m := make(map[string]*types.Type)
		for k, v := range typ.Map {
			m[k] = zonk(v)
		}
		return &types.Type{Kind: types.KindFunc, Map: m, Ord: typ.Ord, Out: zonk(typ.Out)}
	case types.KindVariant:
		// TODO: what is a variant? With that name, it sounds like it should be a
		// dynamic type, but it's not. It wraps the  static type typ.Var. What is
		// it?
		return &types.Type{Kind: types.KindVariant, Var: zonk(typ.Var)}
	default:
		return typ
	}
}

func containsUnificationVariable(typ *types.Type) bool {
	switch typ.Kind {
	case types.KindUnification:
		return true
	case types.KindList:
		return containsUnificationVariable(typ.Val)
	case types.KindMap:
		return containsUnificationVariable(typ.Key) || containsUnificationVariable(typ.Val)
	case types.KindStruct:
		for _, v := range typ.Map {
			if containsUnificationVariable(v) {
				return true
			}
		}
		return false
	case types.KindFunc:
		for _, v := range typ.Map {
			if containsUnificationVariable(v) {
				return true
			}
		}
		return containsUnificationVariable(typ.Out)
	case types.KindVariant:
		// TODO: what's a variant?
		return containsUnificationVariable(typ.Var)
	default:
		return false
	}
}
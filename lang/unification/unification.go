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

// Package unification contains the code related to type unification for the mcl
// language.
package unification

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/purpleidea/mgmt/lang/interfaces"
)

// Unifier holds all the data that the Unify function will need for it to run.
type Unifier struct {
	// AST is the input abstract syntax tree to unify.
	AST interfaces.Stmt

	// Solver is the solver algorithm implementation to use.
	Solver Solver

	// Strategy is a hack to tune unification performance until we have an
	// overall cleaner unification algorithm in place.
	Strategy map[string]string

	Debug bool
	Logf  func(format string, v ...interface{})
}

// Unify takes an AST expression tree and attempts to assign types to every node
// using the specified solver. The expression tree returns a list of invariants
// (or constraints) which must be met in order to find a unique value for the
// type of each expression. This list of invariants is passed into the solver,
// which hopefully finds a solution. If it cannot find a unique solution, then
// it will return an error. The invariants are available in different flavours
// which describe different constraint scenarios. The simplest expresses that a
// a particular node id (it's pointer) must be a certain type. More complicated
// invariants might express that two different node id's must have the same
// type. This function and logic was invented after the author could not find
// any proper literature or examples describing a well-known implementation of
// this process. Improvements and polite recommendations are welcome.
func (obj *Unifier) Unify(ctx context.Context) error {
	if obj.AST == nil {
		return fmt.Errorf("the AST is nil")
	}
	if obj.Solver == nil {
		return fmt.Errorf("the Solver is missing")
	}
	if obj.Logf == nil {
		return fmt.Errorf("the Logf function is missing")
	}

	init := &Init{
		Strategy: obj.Strategy,
		Logf:     obj.Logf,
		Debug:    obj.Debug,
	}
	if err := obj.Solver.Init(init); err != nil {
		return err
	}

	if obj.Debug {
		obj.Logf("tree: %+v", obj.AST)
	}
	invariants, err := obj.AST.Unify()
	if err != nil {
		return err
	}

	// XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX

	//unificationInvariants, typCtx, err := obj.AST.TypeCheck(map[string]*types.Type{}) // ([]*UnificationInvariant, error)
	unificationInvariants, err := obj.AST.TypeCheck() // ([]*UnificationInvariant, error)
	if err != nil {
		return err
	}
	//_ = typCtx

	//Solve(unificationInvariants)

	// Check there are no more ?mark types.

	// SetType on everything...

	// XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX

	// build a list of what we think we need to solve for to succeed
	exprs := []interfaces.Expr{}
	skips := make(map[interfaces.Expr]struct{})
	for _, x := range invariants {
		if si, ok := x.(*interfaces.SkipInvariant); ok {
			skips[si.Expr] = struct{}{}
			continue
		}

		exprs = append(exprs, x.ExprList()...)
	}
	exprMap := ExprListToExprMap(exprs)    // makes searching faster
	exprList := ExprMapToExprList(exprMap) // makes it unique (no duplicates)

	data := &Data{
		Invariants: invariants,
		Expected:   exprList,

		UnificationInvariants: unificationInvariants,
	}
	solved, err := obj.Solver.Solve(ctx, data) // often does union find
	if err != nil {
		return err
	}

	// determine what expr's we need to solve for
	if obj.Debug {
		obj.Logf("expr count: %d", len(exprList))
		//for _, x := range exprList {
		//	obj.Logf("> %p (%+v)", x, x)
		//}
	}

	// XXX: why doesn't `len(exprList)` always == `len(solved.Solutions)` ?
	// XXX: is it due to the extra ExprAny ??? I see an extra function sometimes...

	if obj.Debug {
		obj.Logf("solutions count: %d", len(solved.Solutions))
		//for _, x := range solved.Solutions {
		//	obj.Logf("> %p (%+v) -- %s", x.Expr, x.Type, x.Expr.String())
		//}
	}

	// Determine that our solver produced a solution for every expr that
	// we're interested in. If it didn't, and it didn't error, then it's a
	// bug. We check for this because we care about safety, this ensures
	// that our AST will get fully populated with the correct types!
	for _, x := range solved.Solutions {
		delete(exprMap, x.Expr) // remove everything we know about
	}
	if c := len(exprMap); c > 0 { // if there's anything left, it's bad...
		ptrs := []string{}
		disp := make(map[string]string) // display hack
		for i := range exprMap {
			s := fmt.Sprintf("%p", i) // pointer
			ptrs = append(ptrs, s)
			disp[s] = i.String()
		}
		sort.Strings(ptrs)
		// programming error!
		s := strings.Join(ptrs, ", ")

		obj.Logf("got %d unbound expr's: %s", c, s)
		for i, s := range ptrs {
			obj.Logf("(%d) %s => %s", i, s, disp[s])
		}
		return fmt.Errorf("got %d unbound expr's: %s", c, s)
	}

	if obj.Debug {
		obj.Logf("found a solution!")
	}
	// solver has found a solution, apply it...
	// we're modifying the AST, so code can't error now...
	for _, x := range solved.Solutions {
		if x.Expr == nil {
			// programming error ?
			return fmt.Errorf("unexpected invalid solution at: %p", x)
		}
		if _, exists := skips[x.Expr]; exists {
			continue
		}

		if obj.Debug {
			obj.Logf("solution: %p => %+v\t(%+v)", x.Expr, x.Type, x.Expr.String())
		}
		// apply this to each AST node
		if err := x.Expr.SetType(x.Type); err != nil {
			// programming error!
			// If we error here, it's probably a bug. Likely we
			// should have caught something during type unification,
			// but it slipped through and the function Build API is
			// catching it instead. Try and root cause it to avoid
			// leaving any ghosts in the code.
			return fmt.Errorf("error setting type: %+v, error: %+v", x.Expr, err)
		}
	}
	return nil
}

// InvariantSolution lists a trivial set of EqualsInvariant mappings so that you
// can populate your AST with SetType calls in a simple loop.
type InvariantSolution struct {
	Solutions []*interfaces.EqualsInvariant // list of trivial solutions for each node
}

// ExprList returns the list of valid expressions. This struct is not part of
// the invariant interface, but it implements this anyways.
func (obj *InvariantSolution) ExprList() []interfaces.Expr {
	exprs := []interfaces.Expr{}
	for _, x := range obj.Solutions {
		exprs = append(exprs, x.ExprList()...)
	}
	return exprs
}

// ExclusivesProduct returns a list of different products produced from the
// combinatorial product of the list of exclusives. Each ExclusiveInvariant must
// contain between one and more Invariants. This takes every combination of
// Invariants (choosing one from each ExclusiveInvariant) and returns that list.
// In other words, if you have three exclusives, with invariants named (A1, B1),
// (A2), and (A3, B3, C3) you'll get: (A1, A2, A3), (A1, A2, B3), (A1, A2, C3),
// (B1, A2, A3), (B1, A2, B3), (B1, A2, C3) as results for this function call.
func ExclusivesProduct(exclusives []*interfaces.ExclusiveInvariant) [][]interfaces.Invariant {
	if len(exclusives) == 0 {
		return nil
	}

	length := func(i int) int { return len(exclusives[i].Invariants) }

	// NextIx sets ix to the lexicographically next value,
	// such that for each i > 0, 0 <= ix[i] < length(i).
	NextIx := func(ix []int) {
		for i := len(ix) - 1; i >= 0; i-- {
			ix[i]++
			if i == 0 || ix[i] < length(i) {
				return
			}
			ix[i] = 0
		}
	}

	results := [][]interfaces.Invariant{}

	for ix := make([]int, len(exclusives)); ix[0] < length(0); NextIx(ix) {
		x := []interfaces.Invariant{}
		for j, k := range ix {
			x = append(x, exclusives[j].Invariants[k])
		}
		results = append(results, x)
	}

	return results
}

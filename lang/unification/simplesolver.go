// Mgmt
// Copyright (C) 2013-2023+ James Shubin and the project contributors
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

package unification // TODO: can we put this solver in a sub-package?

import (
	"fmt"
	"sort"

	"github.com/purpleidea/mgmt/lang/interfaces"
	"github.com/purpleidea/mgmt/lang/types"
	"github.com/purpleidea/mgmt/util/errwrap"
)

const (
	// Name is the prefix for our solver log messages.
	Name = "solver: simple"

	// ErrAmbiguous means we couldn't find a solution, but we weren't
	// inconsistent.
	ErrAmbiguous = interfaces.Error("can't unify, no equalities were consumed, we're ambiguous")

	// AllowRecursion specifies whether we're allowed to use the recursive
	// solver or not. It uses an absurd amount of memory, and might hang
	// your system if a simple solution doesn't exist.
	AllowRecursion = false

	// RecursionDepthLimit specifies the max depth that is allowed.
	// FIXME: RecursionDepthLimit is not currently implemented
	RecursionDepthLimit = 5 // TODO: pick a better value ?

	// RecursionInvariantLimit specifies the max number of invariants we can
	// recurse into.
	RecursionInvariantLimit = 5 // TODO: pick a better value ?
)

// SimpleInvariantSolverLogger is a wrapper which returns a
// SimpleInvariantSolver with the log parameter of your choice specified. The
// result satisfies the correct signature for the solver parameter of the
// Unification function.
func SimpleInvariantSolverLogger(logf func(format string, v ...interface{})) func([]interfaces.Invariant, []interfaces.Expr) (*InvariantSolution, error) {
	return func(invariants []interfaces.Invariant, expected []interfaces.Expr) (*InvariantSolution, error) {
		return SimpleInvariantSolver(invariants, expected, logf)
	}
}

// DebugSolverState helps us in understanding the state of the type unification
// solver in a more mainstream format.
// Example:
//
// solver state:
//
// *	str("foo") :: str
// *	call:f(str("foo")) [0xc000ac9f10] :: ?1
// *	var(x) [0xc00088d840] :: ?2
// *	param(x) [0xc00000f950] :: ?3
// *	func(x) { var(x) } [0xc0000e9680] :: ?4
// *	?2 = ?3
// *	?4 = func(arg0 str) ?1
// *	?4 = func(x str) ?2
// *	?1 = ?2
func DebugSolverState(solved map[interfaces.Expr]*types.Type, equalities []interfaces.Invariant) string {
	s := ""

	// all the relevant Exprs
	count := 0
	exprs := make(map[interfaces.Expr]int)
	for _, equality := range equalities {
		for _, expr := range equality.ExprList() {
			count++
			exprs[expr] = count // for sorting
		}
	}

	// print the solved Exprs first
	for expr, typ := range solved {
		s += fmt.Sprintf("%v :: %v\n", expr, typ)
		delete(exprs, expr)
	}

	sortedExprs := []interfaces.Expr{}
	for k := range exprs {
		sortedExprs = append(sortedExprs, k)
	}
	sort.Slice(sortedExprs, func(i, j int) bool { return exprs[sortedExprs[i]] < exprs[sortedExprs[j]] })

	// for each remaining expr, generate a shorter name than the full pointer
	nextVar := 1
	shortNames := map[interfaces.Expr]string{}
	for _, expr := range sortedExprs {
		shortNames[expr] = fmt.Sprintf("?%d", nextVar)
		nextVar++
		s += fmt.Sprintf("%p %v :: %s\n", expr, expr, shortNames[expr])
	}

	// print all the equalities using the short names
	for _, equality := range equalities {
		switch e := equality.(type) {
		case *interfaces.EqualsInvariant:
			_, ok := solved[e.Expr]
			if !ok {
				s += fmt.Sprintf("%s = %v\n", shortNames[e.Expr], e.Type)
			} else {
				// if solved, then this is redundant, don't print anything
			}

		case *interfaces.EqualityInvariant:
			type1, ok1 := solved[e.Expr1]
			type2, ok2 := solved[e.Expr2]
			if !ok1 && !ok2 {
				s += fmt.Sprintf("%s = %s\n", shortNames[e.Expr1], shortNames[e.Expr2])
			} else if ok1 && !ok2 {
				s += fmt.Sprintf("%s = %s\n", type1, shortNames[e.Expr2])
			} else if !ok1 && ok2 {
				s += fmt.Sprintf("%s = %s\n", shortNames[e.Expr1], type2)
			} else {
				// if completely solved, then this is redundant, don't print anything
			}

		case *interfaces.EqualityWrapFuncInvariant:
			funcType, funcOk := solved[e.Expr1]

			args := ""
			argsOk := true
			for i, argName := range e.Expr2Ord {
				if i > 0 {
					args += ", "
				}
				argExpr := e.Expr2Map[argName]
				argType, ok := solved[argExpr]
				if !ok {
					args += fmt.Sprintf("%s %s", argName, shortNames[argExpr])
					argsOk = false
				} else {
					args += fmt.Sprintf("%s %s", argName, argType)
				}
			}

			outType, outOk := solved[e.Expr2Out]

			if !funcOk || !argsOk || !outOk {
				if !funcOk && !outOk {
					s += fmt.Sprintf("%s = func(%s) %s\n", shortNames[e.Expr1], args, shortNames[e.Expr2Out])
				} else if !funcOk && outOk {
					s += fmt.Sprintf("%s = func(%s) %s\n", shortNames[e.Expr1], args, outType)
				} else if funcOk && !outOk {
					s += fmt.Sprintf("%s = func(%s) %s\n", funcType, args, shortNames[e.Expr2Out])
				} else {
					s += fmt.Sprintf("%s = func(%s) %s\n", funcType, args, outType)
				}
			}

		case *interfaces.CallFuncArgsValueInvariant:
			// skip, not used in the examples I care about

		case *interfaces.AnyInvariant:
			// skip, not used in the examples I care about

		case *interfaces.SkipInvariant:
			// we don't care about this one

		default:
			s += fmt.Sprintf("%v\n", equality)
		}
	}

	return s
}

// SimpleInvariantSolver is an iterative invariant solver for AST expressions.
// It is intended to be very simple, even if it's computationally inefficient.
func SimpleInvariantSolver(invariants []interfaces.Invariant, expected []interfaces.Expr, logf func(format string, v ...interface{})) (*InvariantSolution, error) {
	debug := false // XXX: add to interface
	process := func(invariants []interfaces.Invariant) ([]interfaces.Invariant, []*interfaces.ExclusiveInvariant, error) {
		equalities := []interfaces.Invariant{}
		exclusives := []*interfaces.ExclusiveInvariant{}
		generators := []interfaces.Invariant{}

		for ix := 0; len(invariants) > ix; ix++ { // while
			x := invariants[ix]
			switch invariant := x.(type) {
			case *interfaces.EqualsInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.EqualityInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.EqualityInvariantList:
				// de-construct this list variant into a series
				// of equality variants so that our solver can
				// be implemented more simply...
				if len(invariant.Exprs) < 2 {
					return nil, nil, fmt.Errorf("list invariant needs at least two elements")
				}
				for i := 0; i < len(invariant.Exprs)-1; i++ {
					invar := &interfaces.EqualityInvariant{
						Expr1: invariant.Exprs[i],
						Expr2: invariant.Exprs[i+1],
					}
					equalities = append(equalities, invar)
				}

			case *interfaces.EqualityWrapListInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.EqualityWrapMapInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.EqualityWrapStructInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.EqualityWrapFuncInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.EqualityWrapCallInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.GeneratorInvariant:
				// these are special, note the different list
				generators = append(generators, invariant)

			// contains a list of invariants which this represents
			case *interfaces.ConjunctionInvariant:
				invariants = append(invariants, invariant.Invariants...)

			case *interfaces.ExclusiveInvariant:
				// these are special, note the different list
				if len(invariant.Invariants) > 0 {
					exclusives = append(exclusives, invariant)
				}

			case *interfaces.AnyInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.ValueInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.CallFuncArgsValueInvariant:
				equalities = append(equalities, invariant)

			case *interfaces.SkipInvariant:
				// drop it for now
				//equalities = append(equalities, invariant)

			default:
				return nil, nil, fmt.Errorf("unknown invariant type: %T", x)
			}
		}

		// optimization: if we have zero generator invariants, we can
		// discard the value invariants!
		// NOTE: if exclusives do *not* contain nested generators, then
		// we don't need to check for exclusives here, and the logic is
		// much faster and simpler and can possibly solve more cases...
		if len(generators) == 0 && len(exclusives) == 0 {
			used := []int{}
			for i, x := range equalities {
				_, ok1 := x.(*interfaces.ValueInvariant)
				_, ok2 := x.(*interfaces.CallFuncArgsValueInvariant)
				if !ok1 && !ok2 {
					continue
				}
				used = append(used, i) // mark equality as used up
			}
			logf("%s: got %d equalities left after %d used up", Name, len(equalities)-len(used), len(used))
			// delete used equalities, in reverse order to preserve indexing!
			for i := len(used) - 1; i >= 0; i-- {
				ix := used[i] // delete index that was marked as used!
				equalities = append(equalities[:ix], equalities[ix+1:]...)
			}
		}

		// append the generators at the end
		// (they can go in any order, but it's more optimal this way)
		equalities = append(equalities, generators...)

		return equalities, exclusives, nil
	}

	logf("%s: invariants:", Name)
	for i, x := range invariants {
		logf("invariant(%d): %T: %s", i, x, x)
	}

	solved := make(map[interfaces.Expr]*types.Type)
	// iterate through all invariants, flattening and sorting the list...
	equalities, exclusives, err := process(invariants)
	if err != nil {
		return nil, err
	}

	//skipExprs := make(map[interfaces.Expr]struct{})

	// XXX: if these partials all shared the same variable definition, would
	// it all work??? Maybe we don't even need the first map prefix...
	listPartials := make(map[interfaces.Expr]map[interfaces.Expr]*types.Type)
	mapPartials := make(map[interfaces.Expr]map[interfaces.Expr]*types.Type)
	structPartials := make(map[interfaces.Expr]map[interfaces.Expr]*types.Type)
	funcPartials := make(map[interfaces.Expr]map[interfaces.Expr]*types.Type)
	callPartials := make(map[interfaces.Expr]map[interfaces.Expr]*types.Type)

	isSolvedFn := func(solved map[interfaces.Expr]*types.Type) (map[interfaces.Expr]struct{}, bool) {
		unsolved := make(map[interfaces.Expr]struct{})
		result := true
		for _, x := range expected {
			if typ, exists := solved[x]; !exists || typ == nil {
				result = false
				unsolved[x] = struct{}{}
			}
		}
		return unsolved, result
	}

	// list all the expr's connected to expr, use pairs as chains
	listConnectedFn := func(expr interfaces.Expr, exprs []*interfaces.EqualityInvariant) []interfaces.Expr {
		pairsType := pairs(exprs)
		return pairsType.DFS(expr)
	}

	// does the equality invariant already exist in the set? order of expr1
	// and expr2 doesn't matter
	eqContains := func(eq *interfaces.EqualityInvariant, pairs []*interfaces.EqualityInvariant) bool {
		for _, x := range pairs {
			if eq.Expr1 == x.Expr1 && eq.Expr2 == x.Expr2 {
				return true
			}
			if eq.Expr1 == x.Expr2 && eq.Expr2 == x.Expr1 { // reverse
				return true
			}
		}
		return false
	}

	// build a static list that won't get consumed
	eqInvariants := []*interfaces.EqualityInvariant{}
	fnInvariants := []*interfaces.EqualityWrapFuncInvariant{}
	for _, x := range equalities {
		if eq, ok := x.(*interfaces.EqualityInvariant); ok {
			eqInvariants = append(eqInvariants, eq)
		}

		if eq, ok := x.(*interfaces.EqualityWrapFuncInvariant); ok {
			fnInvariants = append(fnInvariants, eq)
		}
	}

	countGenerators := func() (int, int) {
		active := 0
		total := 0
		for _, x := range equalities {
			gen, ok := x.(*interfaces.GeneratorInvariant)
			if !ok {
				continue
			}
			total++ // total count
			if gen.Inactive {
				continue // skip inactive
			}
			active++ // active
		}
		return total, active
	}
	activeGenerators := func() int {
		_, active := countGenerators()
		return active
	}

	logf("%s: starting loop with %d equalities", Name, len(equalities))
	// run until we're solved, stop consuming equalities, or type clash
Loop:
	for {
		// Once we're done solving everything else except the generators
		// then we can exit, but we want to make sure the generators had
		// a chance to "speak up" and make sure they were part of Unify.
		// Every generator gets to run once, and if that does not change
		// the result, then we mark it as inactive.

		logf("%s: iterate...", Name)
		if len(equalities) == 0 && len(exclusives) == 0 && activeGenerators() == 0 {
			break // we're done, nothing left
		}
		used := []int{}
		for eqi := 0; eqi < len(equalities); eqi++ {
			eqx := equalities[eqi]
			logf("%s: match(%T): %+v", Name, eqx, eqx)

			// TODO: could each of these cases be implemented as a
			// method on the Invariant type to simplify this code?
			switch eq := eqx.(type) {
			// trivials
			case *interfaces.EqualsInvariant:
				typ, exists := solved[eq.Expr]
				if !exists {
					solved[eq.Expr] = eq.Type // yay, we learned something!
					used = append(used, eqi)  // mark equality as used up
					logf("%s: solved trivial equality", Name)
					continue
				}
				// we already specified this, so check the repeat is consistent
				if err := typ.Cmp(eq.Type); err != nil {
					// this error shouldn't happen unless we purposefully
					// try to trick the solver, or we're in a recursive try
					return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with equals")
				}
				used = append(used, eqi) // mark equality as duplicate
				logf("%s: duplicate trivial equality", Name)
				continue

			// partials
			case *interfaces.EqualityWrapListInvariant:
				if _, exists := listPartials[eq.Expr1]; !exists {
					listPartials[eq.Expr1] = make(map[interfaces.Expr]*types.Type)
				}

				if typ, exists := solved[eq.Expr1]; exists {
					// wow, now known, so tell the partials!
					// TODO: this assumes typ is a list, is that guaranteed?
					listPartials[eq.Expr1][eq.Expr2Val] = typ.Val
				}

				// can we add to partials ?
				for _, y := range []interfaces.Expr{eq.Expr2Val} {
					typ, exists := solved[y]
					if !exists {
						continue
					}
					t, exists := listPartials[eq.Expr1][y]
					if !exists {
						listPartials[eq.Expr1][y] = typ // learn!

						// Even though this is only a partial learn, we should still add it to the solved information!
						if newTyp, exists := solved[y]; !exists {
							solved[y] = typ // yay, we learned something!
							//used = append(used, i) // mark equality as used up when complete!
							logf("%s: solved partial list val equality", Name)
						} else if err := newTyp.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial list val equality")
						}

						continue
					}
					if err := t.Cmp(typ); err != nil {
						return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial list val")
					}
				}

				// can we solve anything?
				var ready = true // assume ready
				typ := &types.Type{
					Kind: types.KindList,
				}
				valTyp, exists := listPartials[eq.Expr1][eq.Expr2Val]
				if !exists {
					ready = false // nope!
				} else {
					typ.Val = valTyp // build up typ
				}
				if ready {
					if t, exists := solved[eq.Expr1]; exists {
						if err := t.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with list")
						}
					}
					// sub checks
					if t, exists := solved[eq.Expr2Val]; exists {
						if err := t.Cmp(typ.Val); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with list val")
						}
					}

					solved[eq.Expr1] = typ        // yay, we learned something!
					solved[eq.Expr2Val] = typ.Val // yay, we learned something!
					used = append(used, eqi)      // mark equality as used up
					logf("%s: solved list wrap partial", Name)
					continue
				}

			case *interfaces.EqualityWrapMapInvariant:
				if _, exists := mapPartials[eq.Expr1]; !exists {
					mapPartials[eq.Expr1] = make(map[interfaces.Expr]*types.Type)
				}

				if typ, exists := solved[eq.Expr1]; exists {
					// wow, now known, so tell the partials!
					// TODO: this assumes typ is a map, is that guaranteed?
					mapPartials[eq.Expr1][eq.Expr2Key] = typ.Key
					mapPartials[eq.Expr1][eq.Expr2Val] = typ.Val
				}

				// can we add to partials ?
				for _, y := range []interfaces.Expr{eq.Expr2Key, eq.Expr2Val} {
					typ, exists := solved[y]
					if !exists {
						continue
					}
					t, exists := mapPartials[eq.Expr1][y]
					if !exists {
						mapPartials[eq.Expr1][y] = typ // learn!

						// Even though this is only a partial learn, we should still add it to the solved information!
						if newTyp, exists := solved[y]; !exists {
							solved[y] = typ // yay, we learned something!
							//used = append(used, i) // mark equality as used up when complete!
							logf("%s: solved partial map key/val equality", Name)
						} else if err := newTyp.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial map key/val equality")
						}

						continue
					}
					if err := t.Cmp(typ); err != nil {
						return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial map key/val")
					}
				}

				// can we solve anything?
				var ready = true // assume ready
				typ := &types.Type{
					Kind: types.KindMap,
				}
				keyTyp, exists := mapPartials[eq.Expr1][eq.Expr2Key]
				if !exists {
					ready = false // nope!
				} else {
					typ.Key = keyTyp // build up typ
				}
				valTyp, exists := mapPartials[eq.Expr1][eq.Expr2Val]
				if !exists {
					ready = false // nope!
				} else {
					typ.Val = valTyp // build up typ
				}
				if ready {
					if t, exists := solved[eq.Expr1]; exists {
						if err := t.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with map")
						}
					}
					// sub checks
					if t, exists := solved[eq.Expr2Key]; exists {
						if err := t.Cmp(typ.Key); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with map key")
						}
					}
					if t, exists := solved[eq.Expr2Val]; exists {
						if err := t.Cmp(typ.Val); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with map val")
						}
					}

					solved[eq.Expr1] = typ        // yay, we learned something!
					solved[eq.Expr2Key] = typ.Key // yay, we learned something!
					solved[eq.Expr2Val] = typ.Val // yay, we learned something!
					used = append(used, eqi)      // mark equality as used up
					logf("%s: solved map wrap partial", Name)
					continue
				}

			case *interfaces.EqualityWrapStructInvariant:
				if _, exists := structPartials[eq.Expr1]; !exists {
					structPartials[eq.Expr1] = make(map[interfaces.Expr]*types.Type)
				}

				if typ, exists := solved[eq.Expr1]; exists {
					// wow, now known, so tell the partials!
					// TODO: this assumes typ is a struct, is that guaranteed?
					if len(typ.Ord) != len(eq.Expr2Ord) {
						return nil, fmt.Errorf("struct field count differs")
					}
					for i, name := range eq.Expr2Ord {
						expr := eq.Expr2Map[name]                            // assume key exists
						structPartials[eq.Expr1][expr] = typ.Map[typ.Ord[i]] // assume key exists
					}
				}

				// can we add to partials ?
				for name, y := range eq.Expr2Map {
					typ, exists := solved[y]
					if !exists {
						continue
					}
					t, exists := structPartials[eq.Expr1][y]
					if !exists {
						structPartials[eq.Expr1][y] = typ // learn!

						// Even though this is only a partial learn, we should still add it to the solved information!
						if newTyp, exists := solved[y]; !exists {
							solved[y] = typ // yay, we learned something!
							//used = append(used, i) // mark equality as used up when complete!
							logf("%s: solved partial struct field equality", Name)
						} else if err := newTyp.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial struct field equality")
						}

						continue
					}
					if err := t.Cmp(typ); err != nil {
						return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial struct field: %s", name)
					}
				}

				// can we solve anything?
				var ready = true // assume ready
				typ := &types.Type{
					Kind: types.KindStruct,
				}
				typ.Map = make(map[string]*types.Type)
				for name, y := range eq.Expr2Map {
					t, exists := structPartials[eq.Expr1][y]
					if !exists {
						ready = false // nope!
						break
					}
					typ.Map[name] = t // build up typ
				}
				if ready {
					typ.Ord = eq.Expr2Ord // known order

					if t, exists := solved[eq.Expr1]; exists {
						if err := t.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with struct")
						}
					}
					// sub checks
					for name, y := range eq.Expr2Map {
						if t, exists := solved[y]; exists {
							if err := t.Cmp(typ.Map[name]); err != nil {
								return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with struct field: %s", name)
							}
						}
					}

					solved[eq.Expr1] = typ // yay, we learned something!
					// we should add the other expr's in too...
					for name, y := range eq.Expr2Map {
						solved[y] = typ.Map[name] // yay, we learned something!
					}
					used = append(used, eqi) // mark equality as used up
					logf("%s: solved struct wrap partial", Name)
					continue
				}

			case *interfaces.EqualityWrapFuncInvariant:
				if _, exists := funcPartials[eq.Expr1]; !exists {
					funcPartials[eq.Expr1] = make(map[interfaces.Expr]*types.Type)
				}

				if typ, exists := solved[eq.Expr1]; exists {
					// wow, now known, so tell the partials!
					// TODO: this assumes typ is a func, is that guaranteed?
					if len(typ.Ord) != len(eq.Expr2Ord) {
						return nil, fmt.Errorf("func arg count differs")
					}
					for i, name := range eq.Expr2Ord {
						expr := eq.Expr2Map[name]                          // assume key exists
						funcPartials[eq.Expr1][expr] = typ.Map[typ.Ord[i]] // assume key exists
					}
					funcPartials[eq.Expr1][eq.Expr2Out] = typ.Out
				}

				// can we add to partials ?
				for name, y := range eq.Expr2Map {
					typ, exists := solved[y]
					if !exists {
						continue
					}
					t, exists := funcPartials[eq.Expr1][y]
					if !exists {
						funcPartials[eq.Expr1][y] = typ // learn!

						// Even though this is only a partial learn, we should still add it to the solved information!
						if newTyp, exists := solved[y]; !exists {
							solved[y] = typ // yay, we learned something!
							//used = append(used, i) // mark equality as used up when complete!
							logf("%s: solved partial func arg equality", Name)
						} else if err := newTyp.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg equality")
						}

						continue
					}
					if err := t.Cmp(typ); err != nil {
						return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg: %s", name)
					}
				}
				for _, y := range []interfaces.Expr{eq.Expr2Out} {
					typ, exists := solved[y]
					if !exists {
						continue
					}
					t, exists := funcPartials[eq.Expr1][y]
					if !exists {
						funcPartials[eq.Expr1][y] = typ // learn!

						// Even though this is only a partial learn, we should still add it to the solved information!
						if newTyp, exists := solved[y]; !exists {
							solved[y] = typ // yay, we learned something!
							//used = append(used, i) // mark equality as used up when complete!
							logf("%s: solved partial func return equality", Name)
						} else if err := newTyp.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func return equality")
						}

						continue
					}
					if err := t.Cmp(typ); err != nil {
						return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg")
					}
				}

				equivs := listConnectedFn(eq.Expr1, eqInvariants) // or equivalent!
				if debug && len(equivs) > 0 {
					logf("%s: equiv %d: %p %+v", Name, len(equivs), eq.Expr1, eq.Expr1)
					for i, x := range equivs {
						logf("%s: equiv(%d): %p %+v", Name, i, x, x)
					}
				}
				// This determines if a pointer is equivalent to
				// a pointer we're interested to match against.
				inEquiv := func(needle interfaces.Expr) bool {
					for _, x := range equivs {
						if x == needle {
							return true
						}
					}
					return false
				}
				// is there another EqualityWrapFuncInvariant with the same Expr1 pointer?
				for _, fn := range fnInvariants {
					// is this fn.Expr1 related by equivalency graph to eq.Expr1 ?
					if (eq.Expr1 != fn.Expr1) && !inEquiv(fn.Expr1) {
						if debug {
							logf("%s: equiv skip: %p %+v", Name, fn.Expr1, fn.Expr1)
						}
						continue
					}
					if debug {
						logf("%s: equiv used: %p %+v", Name, fn.Expr1, fn.Expr1)
					}
					//if eq.Expr1 != fn.Expr1 { // previously
					//	continue
					//}
					// wow they match or are equivalent

					if len(eq.Expr2Ord) != len(fn.Expr2Ord) {
						return nil, fmt.Errorf("func arg count differs")
					}
					for i := range eq.Expr2Ord {
						lhsName := eq.Expr2Ord[i]
						lhsExpr := eq.Expr2Map[lhsName] // assume key exists
						rhsName := fn.Expr2Ord[i]
						rhsExpr := fn.Expr2Map[rhsName] // assume key exists

						lhsTyp, lhsExists := solved[lhsExpr]
						rhsTyp, rhsExists := solved[rhsExpr]

						// add to eqInvariants if not already there!
						// TODO: If this parent func invariant gets solved,
						// will being unable to add this later be an issue?
						newEq := &interfaces.EqualityInvariant{
							Expr1: lhsExpr,
							Expr2: rhsExpr,
						}
						if !eqContains(newEq, eqInvariants) {
							logf("%s: new equality: %p %+v <-> %p %+v", Name, newEq.Expr1, newEq.Expr1, newEq.Expr2, newEq.Expr2)
							eqInvariants = append(eqInvariants, newEq)
							// TODO: add it as a generator instead?
							equalities = append(equalities, newEq)
						}

						// both solved or both unsolved we skip
						if lhsExists && !rhsExists { // teach rhs
							typ, exists := funcPartials[eq.Expr1][rhsExpr]
							if !exists {
								funcPartials[eq.Expr1][rhsExpr] = lhsTyp // learn!

								// Even though this is only a partial learn, we should still add it to the solved information!
								if newTyp, exists := solved[rhsExpr]; !exists {
									solved[rhsExpr] = lhsTyp // yay, we learned something!
									//used = append(used, i) // mark equality as used up when complete!
									logf("%s: solved partial rhs func arg equality", Name)
								} else if err := newTyp.Cmp(lhsTyp); err != nil {
									return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial rhs func arg equality")
								}

								continue
							}
							if err := typ.Cmp(lhsTyp); err != nil {
								return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg")
							}
						}
						if rhsExists && !lhsExists { // teach lhs
							typ, exists := funcPartials[eq.Expr1][lhsExpr]
							if !exists {
								funcPartials[eq.Expr1][lhsExpr] = rhsTyp // learn!

								// Even though this is only a partial learn, we should still add it to the solved information!
								if newTyp, exists := solved[lhsExpr]; !exists {
									solved[lhsExpr] = rhsTyp // yay, we learned something!
									//used = append(used, i) // mark equality as used up when complete!
									logf("%s: solved partial lhs func arg equality", Name)
								} else if err := newTyp.Cmp(rhsTyp); err != nil {
									return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial lhs func arg equality")
								}

								continue
							}
							if err := typ.Cmp(rhsTyp); err != nil {
								return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg")
							}
						}
					}

					lhsExpr := eq.Expr2Out
					rhsExpr := fn.Expr2Out

					lhsTyp, lhsExists := solved[lhsExpr]
					rhsTyp, rhsExists := solved[rhsExpr]

					// add to eqInvariants if not already there!
					// TODO: If this parent func invariant gets solved,
					// will being unable to add this later be an issue?
					newEq := &interfaces.EqualityInvariant{
						Expr1: lhsExpr,
						Expr2: rhsExpr,
					}
					if !eqContains(newEq, eqInvariants) {
						logf("%s: new equality: %p %+v <-> %p %+v", Name, newEq.Expr1, newEq.Expr1, newEq.Expr2, newEq.Expr2)
						eqInvariants = append(eqInvariants, newEq)
						// TODO: add it as a generator instead?
						equalities = append(equalities, newEq)
					}

					// both solved or both unsolved we skip
					if lhsExists && !rhsExists { // teach rhs
						typ, exists := funcPartials[eq.Expr1][rhsExpr]
						if !exists {
							funcPartials[eq.Expr1][rhsExpr] = lhsTyp // learn!

							// Even though this is only a partial learn, we should still add it to the solved information!
							if newTyp, exists := solved[rhsExpr]; !exists {
								solved[rhsExpr] = lhsTyp // yay, we learned something!
								//used = append(used, i) // mark equality as used up when complete!
								logf("%s: solved partial rhs func return equality", Name)
							} else if err := newTyp.Cmp(lhsTyp); err != nil {
								return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial rhs func return equality")
							}

							continue
						}
						if err := typ.Cmp(lhsTyp); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg")
						}
					}
					if rhsExists && !lhsExists { // teach lhs
						typ, exists := funcPartials[eq.Expr1][lhsExpr]
						if !exists {
							funcPartials[eq.Expr1][lhsExpr] = rhsTyp // learn!

							// Even though this is only a partial learn, we should still add it to the solved information!
							if newTyp, exists := solved[lhsExpr]; !exists {
								solved[lhsExpr] = rhsTyp // yay, we learned something!
								//used = append(used, i) // mark equality as used up when complete!
								logf("%s: solved partial lhs func return equality", Name)
							} else if err := newTyp.Cmp(rhsTyp); err != nil {
								return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial lhs func return equality")
							}

							continue
						}
						if err := typ.Cmp(rhsTyp); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with partial func arg")
						}
					}

				}

				// can we solve anything?
				var ready = true // assume ready
				typ := &types.Type{
					Kind: types.KindFunc,
				}
				typ.Map = make(map[string]*types.Type)
				for name, y := range eq.Expr2Map {
					t, exists := funcPartials[eq.Expr1][y]
					if !exists {
						ready = false // nope!
						break
					}
					typ.Map[name] = t // build up typ
				}
				outTyp, exists := funcPartials[eq.Expr1][eq.Expr2Out]
				if !exists {
					ready = false // nope!
				} else {
					typ.Out = outTyp // build up typ
				}
				if ready {
					typ.Ord = eq.Expr2Ord // known order

					if t, exists := solved[eq.Expr1]; exists {
						if err := t.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with func")
						}
					}
					// sub checks
					for name, y := range eq.Expr2Map {
						if t, exists := solved[y]; exists {
							if err := t.Cmp(typ.Map[name]); err != nil {
								return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with func arg: %s", name)
							}
						}
					}
					if t, exists := solved[eq.Expr2Out]; exists {
						if err := t.Cmp(typ.Out); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with func out")
						}
					}

					solved[eq.Expr1] = typ // yay, we learned something!
					// we should add the other expr's in too...
					for name, y := range eq.Expr2Map {
						solved[y] = typ.Map[name] // yay, we learned something!
					}
					solved[eq.Expr2Out] = typ.Out // yay, we learned something!
					used = append(used, eqi)      // mark equality as used up
					logf("%s: solved func wrap partial", Name)
					continue
				}

			case *interfaces.EqualityWrapCallInvariant:
				// the logic is slightly different here, because
				// we can only go from the func type to the call
				// type as we can't do the reverse determination
				if _, exists := callPartials[eq.Expr2Func]; !exists {
					callPartials[eq.Expr2Func] = make(map[interfaces.Expr]*types.Type)
				}

				if typ, exists := solved[eq.Expr2Func]; exists {
					// wow, now known, so tell the partials!
					if typ.Kind != types.KindFunc {
						return nil, fmt.Errorf("expected: %s, got: %s", types.KindFunc, typ.Kind)
					}
					callPartials[eq.Expr2Func][eq.Expr1] = typ.Out
				}

				typ, ready := callPartials[eq.Expr2Func][eq.Expr1]
				if ready { // ready to solve
					if t, exists := solved[eq.Expr1]; exists {
						if err := t.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with call")
						}
					}
					// sub checks
					if t, exists := solved[eq.Expr2Func]; exists {
						if err := t.Out.Cmp(typ); err != nil {
							return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with call out")
						}
					}

					solved[eq.Expr1] = typ   // yay, we learned something!
					used = append(used, eqi) // mark equality as used up
					logf("%s: solved call wrap partial", Name)
					continue
				}

			// regular matching
			case *interfaces.EqualityInvariant:
				typ1, exists1 := solved[eq.Expr1]
				typ2, exists2 := solved[eq.Expr2]

				if !exists1 && !exists2 { // neither equality connects
					// can't learn more from this equality yet
					// nothing is known about either side of it
					continue
				}
				if exists1 && exists2 { // both equalities already connect
					// both sides are already known-- are they the same?
					if err := typ1.Cmp(typ2); err != nil {
						return nil, errwrap.Wrapf(err, "can't unify, invariant illogicality with equality")
					}
					used = append(used, eqi) // mark equality as used up
					logf("%s: duplicate regular equality", Name)
					continue
				}
				if exists1 && !exists2 { // first equality already connects
					solved[eq.Expr2] = typ1  // yay, we learned something!
					used = append(used, eqi) // mark equality as used up
					logf("%s: solved regular equality", Name)
					continue
				}
				if exists2 && !exists1 { // second equality already connects
					solved[eq.Expr1] = typ2  // yay, we learned something!
					used = append(used, eqi) // mark equality as used up
					logf("%s: solved regular equality", Name)
					continue
				}

				panic("reached unexpected code")

			case *interfaces.GeneratorInvariant:
				// this invariant can generate new ones

				// optimization: we want to run the generators
				// last (but before the exclusives) because
				// they take longer to run. So as long as we've
				// made progress this time around, don't run
				// this just yet, there's still time left...
				if len(used) > 0 {
					continue
				}

				// skip if the inactive flag has been set, as it
				// won't produce any new (novel) inequalities we
				// can use to progress to a result at this time.
				if eq.Inactive {
					continue
				}

				// If this returns nil, we add the invariants
				// it returned and we remove it from the list.
				// If we error, it's because we don't have any
				// new information to provide at this time...
				// XXX: should we pass in `invariants` instead?
				gi, err := eq.Func(equalities, solved)
				if err != nil {
					// set the inactive flag of this generator
					eq.Inactive = true
					continue
				}

				eqs, exs, err := process(gi) // process like at the top
				if err != nil {
					// programming error?
					return nil, errwrap.Wrapf(err, "processing error")
				}
				equalities = append(equalities, eqs...)
				exclusives = append(exclusives, exs...)

				used = append(used, eqi) // mark equality as used up
				logf("%s: solved `generator` equality", Name)
				// reset all other generator equality "inactive" flags
				for _, x := range equalities {
					gen, ok := x.(*interfaces.GeneratorInvariant)
					if !ok {
						continue
					}
					gen.Inactive = false
				}

				continue

			// wtf matching
			case *interfaces.AnyInvariant:
				// this basically ensures that the expr gets solved
				if _, exists := solved[eq.Expr]; exists {
					used = append(used, eqi) // mark equality as used up
					logf("%s: solved `any` equality", Name)
				}
				continue

			case *interfaces.ValueInvariant:
				// don't consume these, they're stored in case
				// a generator invariant wants to read them...
				continue

			case *interfaces.CallFuncArgsValueInvariant:
				// don't consume these, they're stored in case
				// a generator invariant wants to read them...
				continue

			case *interfaces.SkipInvariant:
				//skipExprs[eq.Expr] = struct{}{} // save
				used = append(used, eqi) // mark equality as used up
				continue

			default:
				return nil, fmt.Errorf("unknown invariant type: %T", eqx)
			}
		} // end inner for loop
		if len(used) == 0 && activeGenerators() == 0 {
			// Looks like we're now ambiguous, but if we have any
			// exclusives, recurse into each possibility to see if
			// one of them can help solve this! first one wins. Add
			// in the exclusive to the current set of equalities!

			// To decrease the problem space, first check if we have
			// enough solutions to solve everything. If so, then we
			// don't need to solve any exclusives, and instead we
			// only need to verify that they don't conflict with the
			// found solution, which reduces the search space...

			// Another optimization that can be done before we run
			// the combinatorial exclusive solver, is we can look at
			// each exclusive, and remove the ones that already
			// match, because they don't tell us any new information
			// that we don't already know. We can also fail early
			// if anything proves we're already inconsistent.

			// These two optimizations turn out to use the exact
			// same algorithm and code, so they're combined here...
			_, isSolved := isSolvedFn(solved)
			if isSolved {
				logf("%s: solved early with %d exclusives left!", Name, len(exclusives))
			} else {
				logf("%s: unsolved with %d exclusives left!", Name, len(exclusives))
				if debug {
					for i, x := range exclusives {
						logf("%s: exclusive(%d) left: %s", Name, i, x)
					}
				}
			}

			total, active := countGenerators()
			// we still have work to do for consistency
			if active > 0 {
				continue Loop
			}

			if total > 0 {
				return nil, fmt.Errorf("%d unconsumed generators", total)
			}

			// check for consistency against remaining invariants
			logf("%s: checking for consistency against %d exclusives...", Name, len(exclusives))
			done := []int{}
			for i, invar := range exclusives {
				// test each one to see if at least one works
				match, err := invar.Matches(solved)
				if err != nil {
					logf("exclusive invar failed: %+v", invar)
					return nil, errwrap.Wrapf(err, "inconsistent exclusive")
				}
				if !match {
					continue
				}
				done = append(done, i)
			}
			logf("%s: removed %d consistent exclusives...", Name, len(done))

			// Remove exclusives that matched correctly.
			for i := len(done) - 1; i >= 0; i-- {
				ix := done[i] // delete index that was marked as done!
				exclusives = append(exclusives[:ix], exclusives[ix+1:]...)
			}

			// If we removed any exclusives, then we can start over.
			if len(done) > 0 {
				continue Loop
			}

			// If we don't have any exclusives left, then we don't
			// need the Value invariants... This logic is the same
			// as in process() but it's duplicated here because we
			// want it to happen at this stage as well. We can try
			// and clean up the duplication and improve the logic.
			// NOTE: We should probably check that there aren't any
			// generators left in the equalities, but since we have
			// already tried to use them up, it is probably safe to
			// unblock the solver if it's only ValueInvatiant left.
			if len(exclusives) == 0 || isSolved { // either is okay
				used := []int{}
				for i, x := range equalities {
					_, ok1 := x.(*interfaces.ValueInvariant)
					_, ok2 := x.(*interfaces.CallFuncArgsValueInvariant)
					if !ok1 && !ok2 {
						continue
					}
					used = append(used, i) // mark equality as used up
				}
				logf("%s: got %d equalities left after %d value invariants used up", Name, len(equalities)-len(used), len(used))
				// delete used equalities, in reverse order to preserve indexing!
				for i := len(used) - 1; i >= 0; i-- {
					ix := used[i] // delete index that was marked as used!
					equalities = append(equalities[:ix], equalities[ix+1:]...)
				}

				if len(used) > 0 {
					continue Loop
				}
			}

			if len(exclusives) == 0 && isSolved { // old generators
				used := []int{}
				for i, x := range equalities {
					_, ok := x.(*interfaces.GeneratorInvariant)
					if !ok {
						continue
					}
					used = append(used, i) // mark equality as used up
				}
				logf("%s: got %d equalities left after %d generators used up", Name, len(equalities)-len(used), len(used))
				// delete used equalities, in reverse order to preserve indexing!
				for i := len(used) - 1; i >= 0; i-- {
					ix := used[i] // delete index that was marked as used!
					equalities = append(equalities[:ix], equalities[ix+1:]...)
				}

				if len(used) > 0 {
					continue Loop
				}
			}

			// what have we learned for sure so far?
			partialSolutions := []interfaces.Invariant{}
			logf("%s: %d solved, %d unsolved, and %d exclusives left", Name, len(solved), len(equalities), len(exclusives))
			if len(exclusives) > 0 {
				// FIXME: can we do this loop in a deterministic, sorted way?
				for expr, typ := range solved {
					invar := &interfaces.EqualsInvariant{
						Expr: expr,
						Type: typ,
					}
					partialSolutions = append(partialSolutions, invar)
					logf("%s: solved: %+v", Name, invar)
				}

				// also include anything that hasn't been solved yet
				for _, x := range equalities {
					partialSolutions = append(partialSolutions, x)
					logf("%s: unsolved: %+v", Name, x)
				}
			}
			logf("%s: solver state:\n%s", Name, DebugSolverState(solved, equalities))

			// Lastly, we could loop through each exclusive and see
			// if it only has a single, easy solution. For example,
			// if we know that an exclusive is A or B or C, and that
			// B and C are inconsistent, then we can replace the
			// exclusive with a single invariant and then run that
			// through our solver. We can do this iteratively
			// (recursively for accuracy, but in our case via the
			// simplify method) so that if we're lucky, we rarely
			// need to run the raw exclusive combinatorial solver,
			// which is slow.
			logf("%s: attempting to simplify %d exclusives...", Name, len(exclusives))

			done = []int{} // clear for re-use
			simplified := []interfaces.Invariant{}
			for i, invar := range exclusives {
				// The partialSolutions don't contain any other
				// exclusives... We look at each individually.
				s, err := invar.Simplify(partialSolutions) // XXX: pass in the solver?
				if err != nil {
					logf("exclusive simplification failed: %+v", invar)
					continue
				}
				done = append(done, i)
				simplified = append(simplified, s...)
			}
			logf("%s: simplified %d exclusives...", Name, len(done))

			// Remove exclusives that matched correctly.
			for i := len(done) - 1; i >= 0; i-- {
				ix := done[i] // delete index that was marked as done!
				exclusives = append(exclusives[:ix], exclusives[ix+1:]...)
			}

			// Add new equalities and exclusives onto state globals.
			eqs, exs, err := process(simplified) // process like at the top
			if err != nil {
				// programming error?
				return nil, errwrap.Wrapf(err, "processing error")
			}
			equalities = append(equalities, eqs...)
			exclusives = append(exclusives, exs...)

			// If we removed any exclusives, then we can start over.
			if len(done) > 0 {
				continue Loop
			}

			// TODO: We could try and replace our combinatorial
			// exclusive solver with a real SAT solver algorithm.

			if !AllowRecursion || len(exclusives) > RecursionInvariantLimit {
				logf("%s: %d solved, %d unsolved, and %d exclusives left", Name, len(solved), len(equalities), len(exclusives))
				for i, eq := range equalities {
					logf("%s: (%d) equality: %s", Name, i, eq)
				}
				for i, ex := range exclusives {
					logf("%s: (%d) exclusive: %s", Name, i, ex)
				}

				// these can be very slow, so try to avoid them
				return nil, fmt.Errorf("only recursive solutions left")
			}

			// let's try each combination, one at a time...
			for i, ex := range exclusivesProduct(exclusives) { // [][]interfaces.Invariant
				logf("%s: exclusive(%d):\n%+v", Name, i, ex)
				// we could waste a lot of cpu, and start from
				// the beginning, but instead we could use the
				// list of known solutions found and continue!
				// TODO: make sure none of these edit partialSolutions
				recursiveInvariants := []interfaces.Invariant{}
				recursiveInvariants = append(recursiveInvariants, partialSolutions...)
				recursiveInvariants = append(recursiveInvariants, ex...)
				// FIXME: implement RecursionDepthLimit
				logf("%s: recursing...", Name)
				solution, err := SimpleInvariantSolver(recursiveInvariants, expected, logf)
				if err != nil {
					logf("%s: recursive solution failed: %+v", Name, err)
					continue // no solution found here...
				}
				// solution found!
				logf("%s: recursive solution found!", Name)
				return solution, nil
			}

			// TODO: print ambiguity
			logf("%s: ================ ambiguity ================", Name)
			unsolved, isSolved := isSolvedFn(solved)
			logf("%s: isSolved: %+v", Name, isSolved)
			for _, x := range equalities {
				logf("%s: unsolved equality: %+v", Name, x)
			}
			for x := range unsolved {
				logf("%s: unsolved expected: (%p) %+v", Name, x, x)
			}
			for expr, typ := range solved {
				logf("%s: solved: (%p) => %+v", Name, expr, typ)
			}
			return nil, ErrAmbiguous
		}
		// delete used equalities, in reverse order to preserve indexing!
		for i := len(used) - 1; i >= 0; i-- {
			ix := used[i] // delete index that was marked as used!
			equalities = append(equalities[:ix], equalities[ix+1:]...)
		}
	} // end giant for loop

	// build final solution
	solutions := []*interfaces.EqualsInvariant{}
	// FIXME: can we do this loop in a deterministic, sorted way?
	for expr, typ := range solved {
		// Don't do this here, or the current Unifier struct machinery
		// will see it as a bug. Do it there until we change the API.
		//if _, exists := skipExprs[expr]; exists {
		//	continue
		//}

		invar := &interfaces.EqualsInvariant{
			Expr: expr,
			Type: typ,
		}
		solutions = append(solutions, invar)
	}
	return &InvariantSolution{
		Solutions: solutions,
	}, nil
}

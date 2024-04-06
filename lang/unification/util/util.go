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

// Package util contains some utility functions and algorithms which are useful
// for type unification.
package util

import (
	"fmt"

	"github.com/purpleidea/mgmt/lang/types"
)

// OccursCheck determines if elem exists inside of this type. This is important
// so that we can avoid infinite self-referential types. If we find that it does
// occur, then we error. This: `?1 occurs-in [[?2]]` is what we're doing here!
// This function panics if you pass in a nil type, a malformed type, or an elem
// that has a .Data field that is populated. XXX WHY on the last populated bit?
// XXX .Data contains nil OR type containing question marks, and the outer most type is known. (inner part can contain question marks) OR the whole this known. (rephrase) XXX
func OccursCheck(elem *types.Elem, typ *types.Type) error {
	if typ == nil {
		panic("nil type")
	}

	if typ.Uni != nil {
		root1 := elem.Find()
		root2 := typ.Uni.Find()

		if root1 == root2 {
			return fmt.Errorf("directly in the same set")
		}

		// We don't look at root1.Data as it's not important because we
		// only call OccursCheck if root1.Data (which is the single
		// representative for the elem set by union find) is nil.
		// TODO: Move this check to the top of the function for safety?
		if root1.Data != nil {
			// programming error
			panic("unexpected non-nil Data")
		}

		if root2.Data == nil {
			return nil // We don't occur again, we are done!
		}

		return OccursCheck(root1, root2.Data)
	}
	// Now we know that `typ.Uni == nil`. There could still be a ?1 inside
	// of a recursive type .Val, .Key, etc field. Check through all of them.

	if typ.Kind == types.KindBool {
		return nil // XXX pretty sure this should be nil, check with sam
	}
	if typ.Kind == types.KindStr {
		return nil // XXX pretty sure this should be nil, check with sam
	}
	if typ.Kind == types.KindInt {
		return nil // XXX pretty sure this should be nil, check with sam
	}
	if typ.Kind == types.KindFloat {
		return nil // XXX pretty sure this should be nil, check with sam
	}

	if typ.Kind == types.KindList {
		return OccursCheck(elem, typ.Val)
	}

	if typ.Kind == types.KindMap {
		if err := OccursCheck(elem, typ.Key); err != nil {
			return err
		}
		return OccursCheck(elem, typ.Val)
	}

	if typ.Kind == types.KindStruct {
		for _, k := range typ.Ord {
			t, ok := typ.Map[k]
			if !ok {
				panic("malformed struct order")
			}
			//if t == nil { // checked at the top
			//	panic("malformed struct field")
			//}
			if err := OccursCheck(elem, t); err != nil {
				return err
			}
		}
		return nil
	}

	if typ.Kind == types.KindFunc {
		for _, k := range typ.Ord {
			t, ok := typ.Map[k]
			if !ok {
				panic("malformed func order")
			}
			//if t == nil { // checked at the top
			//	panic("malformed func field")
			//}
			if err := OccursCheck(elem, t); err != nil {
				return err
			}
		}
		return OccursCheck(elem, typ.Out)
	}

	// Unsure if this case is ever used.
	if typ.Kind == types.KindVariant {
		return OccursCheck(elem, typ.Var)
	}

	// programming error
	panic("malformed type")
}

// Unify XXX...
func Unify(typ1, typ2 *types.Type) error {
	if typ1 == nil || typ2 == nil {
		return fmt.Errorf("nil type")
	}

	// Both types are real and don't contain any unification variables, so
	// we just compare them directly.
	if typ1.Uni == nil && typ2.Uni == nil {
		return typ1.Cmp(typ2)
	}

	// Here we have one type that is a ?1 type, and the other one *might* be
	// a full type or it might even contain a ?2 for example. It could be a
	// [?2] or [[?2]] for example.
	if typ1.Uni != nil && typ2.Uni == nil { // aka && typ2.Kind != nil
		root := typ1.Uni.Find()

		// We don't yet know anything about this unification variable.
		if root.Data == nil {
			if err := OccursCheck(root, typ2); err != nil {
				return err
			}

			root.Data = typ2 // learn!
			return nil
		}
		// otherwise, cmp root.Data with typ2

		return Unify(root.Data, typ2)
	}

	// This is the same case as above, except it's the opposite scenario.
	if typ1.Uni == nil && typ2.Uni != nil {
		root := typ2.Uni.Find()

		// We don't yet know anything about this unification variable.
		if root.Data == nil {
			if err := OccursCheck(root, typ1); err != nil {
				return err
			}

			root.Data = typ1 // learn!
			return nil
		}
		// otherwise, cmp root.Data with typ1

		return Unify(root.Data, typ1)
	}

	// Both of these are of the form ?1 and ?2 so we compare them directly.
	if typ1.Uni != nil && typ2.Uni != nil {
		root1 := typ1.Uni.Find()
		root2 := typ2.Uni.Find()

		if root1.Data == nil && root2.Data == nil {
			// XXX: Should I have a general purpose "Merge" function
			// to wrap all Union calls to handle any data instead?
			root1.Union(root2) // merge!
			return nil
		}

		if root1.Data == nil && root2.Data != nil {
			return Unify(typ1, root2.Data)
		}

		if root1.Data != nil && root2.Data == nil {
			return Unify(root1.Data, typ2)
		}

		// root1.Data != nil && root2.Data != nil
		return Unify(root1.Data, root2.Data)
	}

	// At this point, we've handled all the special cases with the ?1, ?2
	// unification variables, so we now expect the kind's to match to unify.
	if k1, k2 := typ1.Kind, typ2.Kind; k1 != k2 {
		return fmt.Errorf("type error: %v != %v", k1, k2)
	}

	if typ1.Kind == types.KindList && typ2.Kind == types.KindList {
		return Unify(typ1.Val, typ2.Val)
	}

	if typ1.Kind == types.KindMap && typ2.Kind == types.KindMap {
		if err := Unify(typ1.Key, typ2.Key); err != nil {
			return err
		}
		return Unify(typ1.Val, typ2.Val)
	}

	if typ1.Kind == types.KindStruct && typ2.Kind == types.KindStruct {
		if typ1.Map == nil || typ2.Map == nil {
			panic("malformed struct type")
		}
		if len(typ1.Ord) != len(typ2.Ord) {
			return fmt.Errorf("struct field count differs")
		}
		for i, k := range typ1.Ord {
			if k != typ2.Ord[i] {
				return fmt.Errorf("struct fields differ")
			}
		}
		for _, k := range typ1.Ord { // loop map in deterministic order
			t1, ok := typ1.Map[k]
			if !ok {
				panic("malformed struct order")
			}
			t2, ok := typ2.Map[k]
			if !ok {
				panic("malformed struct order")
			}
			//if t1 == nil || t2 == nil { // checked at the top
			//	panic("malformed struct field")
			//}
			if err := Unify(t1, t2); err != nil {
				return err
			}
		}
		return nil
	}

	if typ1.Kind == types.KindFunc && typ2.Kind == types.KindFunc {
		if typ1.Map == nil || typ2.Map == nil {
			panic("malformed func type")
		}
		if len(typ1.Ord) != len(typ2.Ord) {
			return fmt.Errorf("func arg count differs")
		}

		// needed for strict cmp only...
		//for i, k := range typ1.Ord {
		//	if k != typ2.Ord[i] {
		//		return fmt.Errorf("func arg differs")
		//	}
		//}
		//for _, k := range typ1.Ord { // loop map in deterministic order
		//	t1, ok := typ1.Map[k]
		//	if !ok {
		//		panic("malformed func order")
		//	}
		//	t2, ok := typ2.Map[k]
		//	if !ok {
		//		panic("malformed func order")
		//	}
		//	//if t1 == nil || t2 == nil { // checked at the top
		//	//	panic("malformed func arg")
		//	//}
		//	if err := Unify(t1, t2); err != nil {
		//		return err
		//	}
		//}

		// if we're not comparing arg names, get the two lists of types
		for i := 0; i < len(typ1.Ord); i++ {
			t1, ok := typ1.Map[typ1.Ord[i]]
			if !ok {
				panic("malformed func order")
			}
			if t1 == nil {
				panic("malformed func arg")
			}

			t2, ok := typ2.Map[typ2.Ord[i]]
			if !ok {
				panic("malformed func order")
			}
			if t2 == nil {
				panic("malformed func arg")
			}

			if err := Unify(t1, t2); err != nil {
				return err
			}
		}

		return Unify(typ1.Out, typ2.Out)
	}

	// Unsure if this case is ever used.
	if typ1.Kind == types.KindVariant && typ2.Kind == types.KindVariant {
		// TODO: should we Unify typ1.Var with typ2.Var ?
		return Unify(typ1.Var, typ2.Var)
	}

	// programming error
	return fmt.Errorf("unhandled type case")
}

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

package funcs

import (
	"fmt"
	"strings"
	"sync"

	"github.com/purpleidea/mgmt/engine"
	"github.com/purpleidea/mgmt/lang/interfaces"
	"github.com/purpleidea/mgmt/lang/types"
	"github.com/purpleidea/mgmt/pgraph"
	"github.com/purpleidea/mgmt/util/errwrap"
)

// State represents the state of a function vertex. This corresponds to an AST
// expr, which is the memory address (pointer) in the graph.
type State struct {
	Expr interfaces.Expr // pointer to the expr vertex

	handle interfaces.Func // the function (if not nil, we've found it on init)

        enabled bool // has EnableVertex() been called yet?
	init   bool // have we run Init on our func?
	ready  bool // has it received all the args it needs at least once?
	loaded bool // has the func run at least once ?
	closed bool // did we close ourself down?

	notify chan struct{} // ping here when new input values exist

	input  chan types.Value // the top level type must be a struct
	output chan types.Value

	mutex *sync.RWMutex // concurrency guard, both for modifying the fields
			    // of this struct and for calling String() and
			    // SetValue() on the associated Expr.

	closeChan chan struct{} // close signal, causing the goroutines started
				// by EnableVertex() to shut down.
	wg        *sync.WaitGroup
}

// Init creates the function state if it can be found in the registered list.
func (obj *State) Init() error {
	handle, err := obj.Expr.Func() // build one and store it, don't re-gen
	if err != nil {
		return err
	}
	if err := handle.Validate(); err != nil {
		return errwrap.Wrapf(err, "could not validate func")
	}
	obj.handle = handle

	sig := obj.handle.Info().Sig
	if sig.Kind != types.KindFunc {
		return fmt.Errorf("must be kind func")
	}
	if len(sig.Ord) > 0 {
		// since we accept input, better get our notification chan built
		obj.notify = make(chan struct{})
	}

	obj.input = make(chan types.Value)  // we close this when we're done
	obj.output = make(chan types.Value) // we create it, func closes it

	obj.mutex = &sync.RWMutex{}

	obj.closeChan = make(chan struct{})
	obj.wg = &sync.WaitGroup{}

	return nil
}

// String satisfies fmt.Stringer so that these print nicely.
func (obj *State) String() string {
	// TODO: use global mutex since it's harder to add state specific mutex
	//obj.mutex.RLock() // prevent race detector issues against SetValue
	//defer obj.mutex.RUnlock()
	// FIXME: also add read locks on any of the children Expr in obj.Expr
	return obj.Expr.String()
}

// Edge links an output vertex (value) to an input vertex with a named argument.
type Edge struct {
	Args []string // list of named args that this edge sends to
}

// String displays the list of arguments this edge satisfies. It is a required
// property to be a valid pgraph.Edge.
func (obj *Edge) String() string {
	return strings.Join(obj.Args, ", ")
}

// Engine represents the running time varying directed acyclic function graph.
type Engine struct {
	Hostname string
	World    engine.World
	Debug    bool
	Logf     func(format string, v ...interface{})

	// Glitch: https://en.wikipedia.org/wiki/Reactive_programming#Glitches
	Glitch bool // allow glitching? (more responsive, but less accurate)

	ag      chan error // used to aggregate fact events without reflect
	agLock  *sync.Mutex
	agCount int // last one turns out the light (closes the ag channel)

        graphMutex *sync.RWMutex        // concurrency guard for the graph
	graph    *pgraph.Graph

	stateMutex *sync.RWMutex       // concurrency guard for the state map
	state map[pgraph.Vertex]*State // state associated with the vertex

	tableMutex *sync.RWMutex            // concurrency guard for the table map
	table map[pgraph.Vertex]types.Value // live table of output values

	loaded     bool          // are all of the funcs loaded?
	loadedChan chan struct{} // funcs loaded signal

	streamChan chan error // signals a new graph can be created or problem

	closeChan chan struct{} // close signal
	wg        *sync.WaitGroup
}

// Init initializes the struct. This is the first call you must make. Do not
// proceed with calls to other methods unless this succeeds first. This also
// loads all the functions by calling Init on each one in the graph.
func (obj *Engine) Init(initialGraph *pgraph.Graph) error {
	obj.ag = make(chan error)
	obj.agLock = &sync.Mutex{}
	obj.stateMutex = &sync.RWMutex{}
	obj.state = make(map[pgraph.Vertex]*State)
	obj.tableMutex = &sync.RWMutex{}
	obj.table = make(map[pgraph.Vertex]types.Value)
	obj.graphMutex = &sync.RWMutex{}
	obj.loadedChan = make(chan struct{})
	obj.streamChan = make(chan error)
	obj.closeChan = make(chan struct{})
	obj.wg = &sync.WaitGroup{}
        var err error
        obj.graph, err = pgraph.NewGraph(initialGraph.Name)
	if err != nil {
		return errwrap.Wrapf(err, "NewGraph failed")
	}

        var errors error
	for _, vertex := range initialGraph.Vertices() {
		expr, ok := vertex.(interfaces.Expr)
		if !ok {
			return fmt.Errorf("vertex (%+v) is not an Expr", vertex)
		}

		e1 := obj.AddVertex(expr)
		e2 := errwrap.Wrapf(e1, "error loading func `%s`", vertex)
		errors = errwrap.Append(errors, e2) // list of errors
	}
	for vertex1, outgoingEdges := range initialGraph.Adjacency() {
		for vertex2, edge := range outgoingEdges {
			e := obj.AddEdge(
				vertex1.(interfaces.Expr),
				vertex2.(interfaces.Expr),
				edge.(*Edge),
			)
			errors = errwrap.Append(errors, e) // list of errors
		}
	}
        if errors != nil { // usually due to `not found` errors
                return errwrap.Wrapf(errors, "could not load requested graph")
        }

	return nil
}

// Validate the graph type checks properly and other tests. Must run Init first.
// This should check that: (1) all vertices have the correct number of inputs,
// (2) that the *Info signatures all match correctly, (3) that the argument
// names match correctly, and that the whole graph is statically correct.
func (obj *Engine) Validate() error {
	inList := func(needle interfaces.Func, haystack []interfaces.Func) bool {
		if needle == nil {
			panic("nil value passed to inList") // catch bugs!
		}
		for _, x := range haystack {
			if needle == x {
				return true
			}
		}
		return false
	}
	var err error
	ptrs := []interfaces.Func{} // Func is a ptr
	defer obj.graphMutex.RUnlock()
	obj.graphMutex.RLock()
	for _, vertex := range obj.graph.Vertices() {
		obj.stateMutex.RLock()
		node := obj.state[vertex]
		obj.stateMutex.RUnlock()
		// TODO: this doesn't work for facts because they're in the Func
		// duplicate pointers would get closed twice, causing a panic...
		if inList(node.handle, ptrs) { // check for duplicate ptrs!
			e := fmt.Errorf("vertex `%s` has duplicate ptr", vertex)
			err = errwrap.Append(err, e)
		}
		ptrs = append(ptrs, node.handle)
	}
	for _, edge := range obj.graph.Edges() {
		if _, ok := edge.(*Edge); !ok {
			e := fmt.Errorf("edge `%s` was not the correct type", edge)
			err = errwrap.Append(err, e)
		}
	}
	if err != nil {
		return err // stage the errors so the user can fix many at once!
	}

	// check if vertices expecting inputs have them
	for vertex, count := range obj.graph.InDegree() {
		obj.stateMutex.RLock()
		node := obj.state[vertex]
		obj.stateMutex.RUnlock()
		if exp := len(node.handle.Info().Sig.Ord); exp != count {
			e := fmt.Errorf("expected %d inputs to `%s`, got %d", exp, node, count)
			if obj.Debug {
				obj.Logf("expected %d inputs to `%s`, got %d", exp, node, count)
				obj.Logf("expected: %+v for `%s`", node.handle.Info().Sig.Ord, node)
			}
			err = errwrap.Append(err, e)
		}
	}

	// expected vertex -> argName
	expected := make(map[*State]map[string]int) // expected input fields
	for vertex1 := range obj.graph.Adjacency() {
		// check for outputs that don't go anywhere?
		//obj.stateMutex.RLock()
		//node1 := obj.state[vertex1]
		//obj.stateMutex.RUnlock()
		//if len(obj.Graph.Adjacency()[vertex1]) == 0 { // no vertex1 -> vertex2
		//	if node1.handle.Info().Sig.Output != nil {
		//		// an output value goes nowhere...
		//	}
		//}
		for vertex2 := range obj.graph.Adjacency()[vertex1] { // populate
			obj.stateMutex.RLock()
			node2 := obj.state[vertex2]
			obj.stateMutex.RUnlock()
			expected[node2] = make(map[string]int)
			for _, key := range node2.handle.Info().Sig.Ord {
				expected[node2][key] = 1
			}
		}
	}

	for vertex1 := range obj.graph.Adjacency() {
		obj.stateMutex.RLock()
		node1 := obj.state[vertex1]
		obj.stateMutex.RUnlock()
		for vertex2, edge := range obj.graph.Adjacency()[vertex1] {
			obj.stateMutex.RLock()
			node2 := obj.state[vertex2]
			obj.stateMutex.RUnlock()
			edge := edge.(*Edge)
			// check vertex1 -> vertex2 (with e) is valid

			for _, arg := range edge.Args { // loop over each arg
				sig := node2.handle.Info().Sig
				if len(sig.Ord) == 0 {
					e := fmt.Errorf("no input expected from `%s` to `%s` with arg `%s`", node1, node2, arg)
					err = errwrap.Append(err, e)
					continue
				}

				if count, exists := expected[node2][arg]; !exists {
					e := fmt.Errorf("wrong input name from `%s` to `%s` with arg `%s`", node1, node2, arg)
					err = errwrap.Append(err, e)
				} else if count == 0 {
					e := fmt.Errorf("duplicate input from `%s` to `%s` with arg `%s`", node1, node2, arg)
					err = errwrap.Append(err, e)
				}
				expected[node2][arg]-- // subtract one use

				out := node1.handle.Info().Sig.Out
				if out == nil {
					e := fmt.Errorf("no output possible from `%s` to `%s` with arg `%s`", node1, node2, arg)
					err = errwrap.Append(err, e)
					continue
				}
				typ, exists := sig.Map[arg] // key in struct
				if !exists {
					// second check of this!
					e := fmt.Errorf("wrong input name from `%s` to `%s` with arg `%s`", node1, node2, arg)
					err = errwrap.Append(err, errwrap.Wrapf(e, "programming error"))
					continue
				}

				if typ.Kind == types.KindVariant { // FIXME: hack for now
					// pass (input arg variants)
				} else if out.Kind == types.KindVariant { // FIXME: hack for now
					// pass (output arg variants)
				} else if typ.Cmp(out) != nil {
					e := fmt.Errorf("type mismatch from `%s` (%s) to `%s` (%s) with arg `%s`", node1, out, node2, typ, arg)
					err = errwrap.Append(err, e)
				}
			}
		}
	}

	// check for leftover function inputs which weren't filled up by outputs
	// (we're trying to call a function with fewer input args than required)
	for node, m := range expected { // map[*State]map[string]int
		for arg, count := range m {
			if count != 0 { // count should be zero if all were used
				e := fmt.Errorf("missing input to `%s` on arg `%s`", node, arg)
				err = errwrap.Append(err, e)
			}
		}
	}

	if err != nil {
		return err // stage the errors so the user can fix many at once!
	}

	return nil
}

func (obj *Engine) AddVertex(vertex interfaces.Expr) error {
	// is this an interface we can use?

	obj.stateMutex.RLock()
	_, exists := obj.state[vertex]
	obj.stateMutex.RUnlock()
	if exists {
		return fmt.Errorf("vertex (%+v) was already added", vertex)
	}

	expr, ok := vertex.(interfaces.Expr)
	if !ok {
		return fmt.Errorf("vertex (%+v) was not an expr", vertex)
	}

	if obj.Debug {
		obj.Logf("Loading func `%s`", vertex)
	}

	state := &State{Expr: expr}
	err := state.Init()
	if err != nil {
		return nil
	}

	obj.stateMutex.Lock()
	obj.state[vertex] = state // store some state!
	obj.stateMutex.Unlock()
	
	obj.graphMutex.Lock()
	obj.graph.AddVertex(vertex)
	obj.graphMutex.Unlock()
	
	return nil
}

func (obj *Engine) AddEdge(v1, v2 interfaces.Expr, e *Edge) error {
	obj.stateMutex.RLock()
        node1 := obj.state[v1]
        node2 := obj.state[v2]
	obj.stateMutex.RUnlock()

	node1.mutex.RLock()
	enabled1 := node1.enabled
	node1.mutex.RUnlock()
        if enabled1 {
		return fmt.Errorf("cannot add edge because vertex (%+v) is already enabled", v1)
        }

	node2.mutex.RLock()
	enabled2 := node2.enabled
	node2.mutex.RUnlock()
        if enabled2 {
		return fmt.Errorf("cannot add edge because vertex (%+v) is already enabled", v2)
        }

	obj.graphMutex.Lock()
	obj.graph.AddEdge(v1, v2, e)
	obj.graphMutex.Unlock()
	
	return nil
}

// Each node can have multiple input edges, and can be connected to multiple
// output edges. The node needs a value from every input in order to process
// this incoming data, and once the node produces an output, that output value
// is sent along every output edge. To allow the node to focus on transforming
// input values to output values, the work is divided among three goroutines
// spawned by Engine::EnableVertex():
//
// 1. Waiting until every input edge has provided at least one value, and
//    combining those values into a single struct.
// 2. The node's Expr::Stream() function.
// 3. Copying the output value to every output edge.
//
// Those goroutines can be terminated in two different ways:
//
// 1. When Engine::RemoveVertex() is called, the State's closeChan channel is closed,
// and only the goroutines for this node terminate.
// 2. When Engine::Close() is called, the Engine's closeChan channel is closed,
// and the goroutines for all the nodes terminate.
//
// For constants, the goroutines terminate earlier on their own once there is
// no more work to do.
func (obj *Engine) EnableVertex(vertex interfaces.Expr) error {
        // TODO: cause the upstream and downstream goroutines to refresh their edges

	obj.stateMutex.Lock()
	node := obj.state[vertex]
        enabled := node.enabled
        node.enabled = true
	obj.stateMutex.Unlock()
        if enabled {
		return fmt.Errorf("vertex (%+v) already enabled", vertex)
        }
        
	if obj.Debug {
		obj.SafeLogf("Startup func `%s`", node)
	}

	init := &interfaces.Init{
		Hostname: obj.Hostname,
		Input:    node.input,
		Output:   node.output,
		World:    obj.World,
		Debug:    obj.Debug,
		Logf: func(format string, v ...interface{}) {
			obj.Logf("func: "+format, v...)
		},
	}
	if err := node.handle.Init(init); err != nil {
		return errwrap.Wrapf(err, "could not init func `%s`", node)
	}
        node.mutex.Lock()
	node.init = true // we've successfully initialized
        node.mutex.Unlock()

	// First goroutine: waiting until every input edge has provided at
	// least one value, and combining those values into a single struct.
	obj.graphMutex.RLock()
	incoming := obj.graph.IncomingGraphVertices(vertex) // []Vertex
	obj.graphMutex.RUnlock()
	if len(incoming) == 0 {
		// For constant nodes, we can terminate the first goroutine
		// early (we don't even bother spawning it at all). Close
		// Expr::Stream()'s input channel in order to give the second
		// goroutine an opportunity to end early as well.
		close(node.input)
	} else {
		// process function input data
		obj.wg.Add(1)
                node.wg.Add(1)
		go func(vertex interfaces.Expr) {
			obj.stateMutex.RLock()
			node := obj.state[vertex]
			obj.stateMutex.RUnlock()
			defer obj.wg.Done()
			defer node.wg.Done()
			defer close(node.input)	// When the first goroutine
						// terminates, close
						// Expr::Stream()'s input
						// channel so that the second
						// goroutine terminates as
						// well.
			var ready  bool
			for {
				select {
				case <- node.notify:
					// New input values are available, but
					// we aren't ready to forward those
					// inputs to the node until we have a
					// starting value for _all_ the inputs.
					// Once we do, we forward the new
					// combination of inputs to the node
					// each time _one_ of the inputs
					// change.

					ready = true	// Later set to false if any of
							// the inputs is missing a
							// starting value.
					si := &types.Type{
						// input to functions are structs
						Kind: types.KindStruct,
						Map:  node.handle.Info().Sig.Map,
						Ord:  node.handle.Info().Sig.Ord,
					}
					st := types.NewStruct(si)
					for _, v := range incoming {
						obj.graphMutex.RLock()
						args := obj.graph.Adjacency()[v][vertex].(*Edge).Args
						obj.graphMutex.RUnlock()
						obj.stateMutex.RLock()
						from := obj.state[v]
						obj.stateMutex.RUnlock()
						obj.tableMutex.RLock()
						value, exists := obj.table[v]
						obj.tableMutex.RUnlock()
						if !exists {
							ready = false
							break
						}

						// set each arg, since one value
						// could get used for multiple
						// function inputs (shared edge)
						for _, arg := range args {
							err := st.Set(arg, value) // populate struct
							if err != nil {
								node.mutex.RLock()
								msg := fmt.Sprintf("struct set failure on `%s` from `%s`: %v", node, from, err)
								node.mutex.RUnlock()
								panic(msg)
							}
						}
					}
					if ready {
						// Send the struct to Expr::Stream()
						select {
						case node.input <- st:

						case <-obj.closeChan:
							return
						case <-node.closeChan:
							return
						}
					}

				case <-obj.closeChan:
					return
				case <-node.closeChan:
					return
				}
			}
		}(vertex)
	}

	// Second goroutine: the node's Expr::Stream() function.
        //
	// Note that this goroutine is blocked on the Expr::Stream() call for
	// most of its execution, so closing either closeChan is not going to
	// immediately terminate this goroutine. However, closing either
	// closeChan is going to terminate the first goroutine, which is going
	// to close the state.input channel, which is going to signal to Expr::
	// Stream() that it should return.
	obj.wg.Add(1)
	node.wg.Add(1)
	go func(vertex interfaces.Expr) {
		obj.stateMutex.RLock()
		node := obj.state[vertex]
		obj.stateMutex.RUnlock()
		defer obj.wg.Done()
		defer node.wg.Done()
		if obj.Debug {
			node.mutex.RLock()
			obj.SafeLogf("Running func `%s`", node)
			node.mutex.RUnlock()
		}
		err := node.handle.Stream()
		if obj.Debug {
			node.mutex.RLock()
			obj.SafeLogf("Exiting func `%s`", node)
			node.mutex.RUnlock()
		}
		if err != nil {
			// Expr::Stream() ended with an error
			node.mutex.RLock()
			err := errwrap.Wrapf(err, "problem streaming func `%s`", node)
			node.mutex.RUnlock()
			select {
			case obj.ag <- err: // send to aggregate channel

			case <-obj.closeChan:
				return
			case <-node.closeChan:
				return
			}
		}
	}(vertex)

	// Third goroutine: copying the output value to every output edge.
	obj.wg.Add(1)
	node.wg.Add(1)
	go func(vertex interfaces.Expr) { // process function output data
		obj.stateMutex.RLock()
		node := obj.state[vertex]
		obj.stateMutex.RUnlock()
		defer obj.wg.Done()
		defer node.wg.Done()
		defer obj.agDone(vertex)
		obj.graphMutex.RLock()
		outgoing := obj.graph.OutgoingGraphVertices(vertex) // []Vertex
		obj.graphMutex.RUnlock()
		for {
			select {
			case value, ok := <- node.output:
				if ok {
					// Expr::Stream() emitted an output value.

					obj.tableMutex.RLock()
					cached, exists := obj.table[vertex]
					obj.tableMutex.RUnlock()
					if !exists { // first value received
						// RACE: do this AFTER value is present!
						//node.loaded = true // not yet please
						node.mutex.RLock()
						obj.Logf("func `%s` started", node)
						node.mutex.RUnlock()
					} else if value.Cmp(cached) == nil {
						// skip if new value is same as previous
						// if this happens often, it *might* be
						// a bug in the function implementation
						// FIXME: do we need to disable engine
						// caching when using hysteresis?
						node.mutex.RLock()
						obj.Logf("func `%s` skipped", node)
						node.mutex.RUnlock()
						continue
					}
					obj.tableMutex.Lock()
					// XXX: maybe we can get rid of the table...
					obj.table[vertex] = value // save the latest
					node.mutex.Lock()
					if err := node.Expr.SetValue(value); err != nil {
						msg := fmt.Sprintf("could not set value for `%s`: %+v", node, err)
						node.mutex.Unlock()
						panic(msg)
					}
					node.loaded = true // set *after* value is in :)
					obj.Logf("func `%s` changed", node)
					node.mutex.Unlock()
					obj.tableMutex.Unlock()

					// FIXME: will this actually prevent glitching?
					// if we only notify the aggregate channel when
					// we're at the bottom of the topo sort (eg: no
					// outgoing vertices to notify) then we'll have
					// a glitch free subtree in the programs ast...
					if obj.Glitch || len(outgoing) == 0 {
						select {
						case obj.ag <- nil: // send to aggregate channel

						case <-obj.closeChan:
							return
						case <-node.closeChan:
							return
						}
					}

					// notify the receiving vertices
					for _, v := range outgoing {
						obj.stateMutex.RLock()
						node := obj.state[v]
						obj.stateMutex.RUnlock()
						select {
						case node.notify <- struct{}{}:

						case <-obj.closeChan:
							return
						case <-node.closeChan:
							return
						}
					}
				} else {
					// Expr::Stream() will no longer emit
					// any output values.

					node.mutex.RLock()
					obj.Logf("func `%s` stopped", node)
					node.mutex.RUnlock()

					// nodes that never loaded will cause the engine to hang
					if !node.loaded {
						node.mutex.RLock()
						e := fmt.Errorf("func `%s` stopped before it was loaded", node)
						node.mutex.RUnlock()
						select {
						case obj.ag <- e:
						case <-obj.closeChan:
							return
						case <-node.closeChan:
							return
						}
					}
				}

			case <-obj.closeChan:
				return
			case <-node.closeChan:
				return
			}
		}
	}(vertex)

	return nil
}

// Also terminates the associated goroutines ("disables" the vertex), and
// removes the incoming and outgoing edges.
func (obj *Engine) RemoveVertex(v interfaces.Expr) {
	obj.stateMutex.RLock()
	node := obj.state[v]
	obj.stateMutex.RUnlock()

	node.closeChan <- struct{}{}
	node.wg.Wait()
	
        obj.graphMutex.Lock()
        obj.graph.DeleteVertex(v)
        obj.graphMutex.Unlock()

        // TODO: cause the upstream and downstream goroutines to refresh their edges
}


// Run starts up this function engine and gets it all running. It errors if the
// startup failed for some reason. On success, use the Stream and Table methods
// for future interaction with the engine, and the Close method to shut it off.
func (obj *Engine) Run() error {
	obj.graphMutex.RLock()
	topologicalSort, err := obj.graph.TopologicalSort()
	obj.graphMutex.RUnlock()
        if err != nil {
		return err
        }

	if len(topologicalSort) == 0 { // no funcs to load!
		close(obj.loadedChan)
		close(obj.streamChan)
		return nil
	}

	// TODO: build a timer that runs while we wait for all funcs to startup.
	// after some delay print a message to tell us which funcs we're waiting
	// for to startup and that they are slow and blocking everyone, and then
	// fail permanently after the timeout so that bad code can't block this!

	// Enable all the funcs, causing them to start sending values to each
	// other.
	obj.agAdd(len(topologicalSort))
	for _, vertex := range topologicalSort {
		err := obj.EnableVertex(vertex.(interfaces.Expr))
		if err != nil {
			return err
		}
	}

	// send event on streamChan when any of the (aggregated) facts change
	obj.wg.Add(1)
	go func() {
		defer obj.wg.Done()
		defer close(obj.streamChan)
	Loop:
		for {
			var err error
			var ok bool
			select {
			case err, ok = <-obj.ag: // aggregated channel
				if !ok {
					break Loop // channel shutdown
				}

				if !obj.loaded {
					// now check if we're ready
					var loaded = true // initially assume true
					for _, vertex := range topologicalSort {
						obj.stateMutex.RLock()
						node := obj.state[vertex]
						obj.stateMutex.RUnlock()
						node.mutex.RLock()
						nodeLoaded := node.loaded
						node.mutex.RUnlock()
						if !nodeLoaded {
							loaded = false // we were wrong
							// TODO: add better "not loaded" reporting
							if obj.Debug {
								node.mutex.RLock()
								obj.Logf("not yet loaded: %s", node)
								node.mutex.RUnlock()
							}
							break
						}
					}
					obj.loaded = loaded

					if obj.loaded {
						// this causes an initial start
						// signal to be sent out below,
						// since the stream sender runs
						if obj.Debug {
							obj.Logf("loaded")
						}
						close(obj.loadedChan) // signal
					} else {
						if err == nil {
							continue // not ready to send signal
						} // pass errors through...
					}
				}

			case <-obj.closeChan:
				return
			}

			// send stream signal
			select {
			// send events or errors on streamChan, eg: func failure
			case obj.streamChan <- err: // send
				if err != nil {
					return
				}
			case <-obj.closeChan:
				return
			}
		}
	}()

	return nil
}

// agAdd registers a user on the ag channel.
func (obj *Engine) agAdd(i int) {
	defer obj.agLock.Unlock()
	obj.agLock.Lock()
	obj.agCount += i
}

// agDone closes the channel if we're the last one using it.
func (obj *Engine) agDone(vertex interfaces.Expr) {
	defer obj.agLock.Unlock()
	obj.agLock.Lock()

	obj.stateMutex.RLock()
	node := obj.state[vertex]
	obj.stateMutex.RUnlock()

        node.mutex.Lock()
	node.closed = true
        node.mutex.Unlock()

	// FIXME: (perf) cache this into a table which we narrow down with each
	// successive call. look at the outgoing vertices that I would affect...
        defer obj.graphMutex.RUnlock()
        obj.graphMutex.RLock()
	for _, v := range obj.graph.OutgoingGraphVertices(vertex) { // close for each one
		// now determine who provides inputs to that vertex...
		var closed = true
		for _, vv := range obj.graph.IncomingGraphVertices(v) {
			// are they all closed?
                        obj.stateMutex.RLock()
                        isClosed := obj.state[vv].closed
                        obj.stateMutex.RUnlock()
			if !isClosed {
				closed = false
				break
			}
		}
		if closed { // if they're all closed, we can close the input
			close(obj.state[v].notify)
		}
	}

	if obj.agCount == 0 {
		close(obj.ag)
	}
}

// RLock takes a read lock on the data that gets written to the AST, so that
// interpret can be run without anything changing part way through.
func (obj *Engine) RLock() {
	obj.tableMutex.RLock()
}

// RUnlock frees a read lock on the data that gets written to the AST, so that
// interpret can be run without anything changing part way through.
func (obj *Engine) RUnlock() {
	obj.tableMutex.RUnlock()
}

// SafeLogf logs a message, although it adds a read lock around the logging in
// case a `node` argument is passed in which would set off the race detector.
func (obj *Engine) SafeLogf(format string, v ...interface{}) {
	// We're adding a global mutex, because it's harder to only isolate the
	// individual node specific mutexes needed since it may contain others!
	if len(v) > 0 {
		obj.tableMutex.RLock()
	}
	obj.Logf(format, v...)
	if len(v) > 0 {
		obj.tableMutex.RUnlock()
	}
}

// Stream returns a channel of engine events. Wait for nil events to know when
// the Table map has changed. An error event means this will shutdown shortly.
// Do not run the Table function before we've received one non-error event.
func (obj *Engine) Stream() chan error {
	return obj.streamChan
}

// Close shuts down the function engine. It waits till everything has finished.
func (obj *Engine) Close() error {
	var err error
        obj.graphMutex.RLock()
	topologicalSort, err := obj.graph.TopologicalSort()
        obj.graphMutex.RUnlock()
	if err != nil {
		return err
	}

	for _, vertex := range topologicalSort { // FIXME: should we do this in reverse?
		obj.stateMutex.RLock()
		node := obj.state[vertex]
		obj.stateMutex.RUnlock()
		if node.init { // did we Init this func?
			if e := node.handle.Close(); e != nil {
				node.mutex.RLock()
				e := errwrap.Wrapf(e, "problem closing func `%s`", node)
				node.mutex.RUnlock()
				err = errwrap.Append(err, e) // list of errors
			}
		}
	}
	close(obj.closeChan)
	obj.wg.Wait() // wait so that each func doesn't need to do this in close
	return err
}

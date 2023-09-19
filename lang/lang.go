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

// Package lang is the mcl language frontend that implements the reactive DSL
// that lets users model their desired state over time.
package lang

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/purpleidea/mgmt/engine"
	"github.com/purpleidea/mgmt/lang/ast"
	_ "github.com/purpleidea/mgmt/lang/funcs/core" // import so the funcs register
	"github.com/purpleidea/mgmt/lang/funcs/dage"
	"github.com/purpleidea/mgmt/lang/funcs/vars"
	"github.com/purpleidea/mgmt/lang/inputs"
	"github.com/purpleidea/mgmt/lang/interfaces"
	"github.com/purpleidea/mgmt/lang/interpolate"
	"github.com/purpleidea/mgmt/lang/interpret"
	"github.com/purpleidea/mgmt/lang/parser"
	"github.com/purpleidea/mgmt/lang/unification"
	"github.com/purpleidea/mgmt/pgraph"
	"github.com/purpleidea/mgmt/util"
	"github.com/purpleidea/mgmt/util/errwrap"
)

// Lang is the main language lexer/parser object.
type Lang struct {
	Fs    engine.Fs // connected fs where input dir or metadata exists
	FsURI string

	// Input is a string which specifies what the lang should run. It can
	// accept values in several different forms. If is passed a single dash
	// (-), then it will use `os.Stdin`. If it is passed a single .mcl file,
	// then it will attempt to run that. If it is passed a directory path,
	// then it will attempt to run from there. Instead, if it is passed the
	// path to a metadata file, then it will attempt to parse that and run
	// from that specification. If none of those match, it will attempt to
	// run the raw string as mcl code.
	Input string

	Hostname string
	World    engine.World
	Prefix   string
	Debug    bool
	Logf     func(format string, v ...interface{})

	ast   interfaces.Stmt // store main prog AST here
	funcs *dage.Engine    // function event engine

	loadedChan chan struct{} // loaded signal

	streamChan chan error // signals a new graph can be created or problem
	//streamBurst bool // should we try and be bursty with the stream events?

	wg *sync.WaitGroup
}

// Init initializes the lang struct, and starts up the initial input parsing.
// NOTE: The trick is that we need to get the list of funcs to watch AND start
// watching them, *before* we pull their values, that way we'll know if they
// changed from the values we wanted.
func (obj *Lang) Init() error {
	if obj.Debug {
		obj.Logf("input: %s", obj.Input)
		tree, err := util.FsTree(obj.Fs, "/") // should look like gapi
		if err != nil {
			return err
		}
		obj.Logf("run tree:\n%s", tree)
	}

	// we used to support stdin passthrough, but we we got rid of it for now
	// the fs input here is the local fs we're reading to get the files from
	// which is usually etcdFs.
	output, err := inputs.ParseInput(obj.Input, obj.Fs)
	if err != nil {
		return errwrap.Wrapf(err, "could not activate an input parser")
	}
	if len(output.Workers) > 0 {
		// either programming error, or someone hacked in something here
		// by the time *this* ParseInput runs, we should be standardized
		return fmt.Errorf("input contained file system workers")
	}
	reader := bytes.NewReader(output.Main)

	// no need to run recursion detection since this is the beginning
	// TODO: do the paths need to be cleaned for "../" before comparison?

	// run the lexer/parser and build an AST
	obj.Logf("lexing/parsing...")
	// this reads an io.Reader, which might be a stream of multiple files...
	xast, err := parser.LexParse(reader)
	if err != nil {
		return errwrap.Wrapf(err, "could not generate AST")
	}
	if obj.Debug {
		obj.Logf("behold, the AST: %+v", xast)
	}

	importGraph, err := pgraph.NewGraph("importGraph")
	if err != nil {
		return err
	}
	importVertex := &pgraph.SelfVertex{
		Name:  "",          // first node is the empty string
		Graph: importGraph, // store a reference to ourself
	}
	importGraph.AddVertex(importVertex)

	obj.Logf("init...")
	// init and validate the structure of the AST
	data := &interfaces.Data{
		// TODO: add missing fields here if/when needed
		Fs:       obj.Fs,
		FsURI:    obj.FsURI,
		Base:     output.Base, // base dir (absolute path) the metadata file is in
		Files:    output.Files,
		Imports:  importVertex,
		Metadata: output.Metadata,
		Modules:  "/" + interfaces.ModuleDirectory, // do not set from env for a deploy!

		LexParser:       parser.LexParse,
		Downloader:      nil, // XXX: is this used here?
		StrInterpolater: interpolate.StrInterpolate,
		//World: obj.World, // TODO: do we need this?

		Prefix: obj.Prefix,
		Debug:  obj.Debug,
		Logf: func(format string, v ...interface{}) {
			// TODO: is this a sane prefix to use here?
			obj.Logf("ast: "+format, v...)
		},
	}
	// some of this might happen *after* interpolate in SetScope or Unify...
	if err := xast.Init(data); err != nil {
		return errwrap.Wrapf(err, "could not init and validate AST")
	}

	obj.Logf("interpolating...")
	// interpolate strings and other expansionable nodes in AST
	iast, err := xast.Interpolate()
	if err != nil {
		return errwrap.Wrapf(err, "could not interpolate AST")
	}
	obj.ast = iast

	variables := map[string]interfaces.Expr{
		"purpleidea": &ast.ExprStr{V: "hello world!"}, // james says hi
		// TODO: change to a func when we can change hostname dynamically!
		"hostname": &ast.ExprStr{V: obj.Hostname},
	}
	consts := ast.VarPrefixToVariablesScope(vars.ConstNamespace) // strips prefix!
	addback := vars.ConstNamespace + interfaces.ModuleSep        // add it back...
	variables, err = ast.MergeExprMaps(variables, consts, addback)
	if err != nil {
		return errwrap.Wrapf(err, "couldn't merge in consts")
	}

	// top-level, built-in, initial global scope
	scope := &interfaces.Scope{
		Variables: variables,
		// all the built-in top-level, core functions enter here...
		Functions: ast.FuncPrefixToFunctionsScope(""), // runs funcs.LookupPrefix
	}

	obj.Logf("building scope...")
	// propagate the scope down through the AST...
	if err := obj.ast.SetScope(scope); err != nil {
		return errwrap.Wrapf(err, "could not set scope")
	}

	// apply type unification
	logf := func(format string, v ...interface{}) {
		if obj.Debug { // unification only has debug messages...
			obj.Logf("unification: "+format, v...)
		}
	}
	obj.Logf("running type unification...")
	unifier := &unification.Unifier{
		AST:    obj.ast,
		Solver: unification.SimpleInvariantSolverLogger(logf),
		Debug:  obj.Debug,
		Logf:   logf,
	}
	if err := unifier.Unify(); err != nil {
		return errwrap.Wrapf(err, "could not unify types")
	}

	obj.Logf("building function graph...")
	// we assume that for some given code, the list of funcs doesn't change
	// iow, we don't support variable, variables or absurd things like that
	graph := &pgraph.Graph{Name: "functionGraph"}
	env := make(map[string]interfaces.Func)
	for k, v := range scope.Variables {
		g, builtinFunc, err := v.MergedGraph(nil)
		if err != nil {
			return errwrap.Wrapf(err, "calling MergedGraph on builtins")
		}
		graph.AddGraph(g)
		env[k] = builtinFunc
	}
	g, err := obj.ast.MergedGraph(env) // build the graph of functions
	if err != nil {
		return errwrap.Wrapf(err, "could not generate function graph")
	}
	graph.AddGraph(g)

	if obj.Debug {
		obj.Logf("function graph: %+v", obj.graph)
		obj.graph.Logf(obj.Logf) // log graph output with this logger...
		//if err := obj.graph.ExecGraphviz("/tmp/graphviz.dot"); err != nil {
		//	return errwrap.Wrapf(err, "writing graph failed")
		//}
	}

	if graph.NumVertices() == 0 { // no funcs to load!
		// send only one signal since we won't ever send after this!
		obj.Logf("static graph found")
		obj.wg.Add(1)
		go func() {
			defer obj.wg.Done()
			defer close(obj.streamChan) // no more events are coming!
			close(obj.loadedChan)       // signal
			select {
			case obj.streamChan <- nil: // send one signal
				// pass
			case <-obj.closeChan:
				return
			}
		}()
		return nil // exit early, no funcs to load!
	}

	obj.funcs = &dage.Engine{
		Name:     "lang", // TODO: arbitrary name for now
		Hostname: obj.Hostname,
		World:    obj.World,
		Debug:    obj.Debug,
		Logf: func(format string, v ...interface{}) {
			obj.Logf("funcs: "+format, v...)
		},
	}

	obj.Logf("function engine initializing...")
	if err := obj.funcs.Setup(); err != nil {
		return errwrap.Wrapf(err, "init error with func engine")
	}
	//defer obj.funcs.Cleanup() // run in Close()?

	//obj.Logf("function engine validating...")
	//if err := obj.funcs.Validate(); err != nil {
	//	return errwrap.Wrapf(err, "validate error with func engine")
	//}

	obj.Logf("function engine starting...")
	//XXX	// On failure, we expect the caller to run Close() to shutdown all of
	//XXX	// the currently initialized (and running) funcs... This is needed if
	//XXX	// we successfully ran `Run` but isn't needed only for Init/Validate.
	//XXX: maybe instead on shutdown we Lock() and DeleteVertex everything from the graph?

	errChan := make(chan error)
	//wg := &sync.WaitGroup{}
	//defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//wg.Add(1)
	obj.wg.Add(1)
	go func() {
		//defer close(errChan) // happens never!
		defer obj.wg.Done()
		//defer wg.Done()
		err := obj.funcs.Run(ctx)
		if err == nil {
			return
		}
		// Run() should only error if not a dag I think...
		select {
		case errChan <- errwrap.Wrapf(err, "run error with func engine"):
		case <-ctx.Done():
			// unblock
		}
	}()

	<-obj.funcs.Started() // wait for startup (will not block forever)

	obj.funcs.Lock()
	if err := obj.funcs.AddGraph(graph); err != nil { // add all of this graph into the func engine
		return errwrap.Wrapf(err, "error adding to function graph engine")
	}
	obj.funcs.Unlock()

	// wait for some activity
	obj.Logf("stream...")

			case err, ok = <-errChan:
				if !ok {
					obj.Logf("error chan closed")
					//return
					continue
				}

			case <-obj.closeChan:
				return
			}

	return nil
}

// Stream returns a channel of graph change requests or errors. These are
// usually sent when a func output changes.
func (obj *Lang) Stream() <-chan error {
	return obj.streamChan
}

// Interpret runs the interpreter and returns a graph and corresponding error.
func (obj *Lang) Interpret() (*pgraph.Graph, error) {
	select {
	case <-obj.funcs.Loaded(): // funcs are now loaded!
		// pass
	default:
		// if this is hit, someone probably called this too early!
		// it should only be called in response to a stream event!
		return nil, fmt.Errorf("funcs aren't loaded yet")
	}

	obj.Logf("running interpret...")
	table := obj.funcs.Table() // map[pgraph.Vertex]types.Value

	// this call returns the graph
	graph, err := interpret.Interpret(obj.ast, table)
	if err != nil {
		return nil, errwrap.Wrapf(err, "could not interpret")
	}

	return graph, nil // return a graph
}

// Close shuts down the lang struct and causes all the funcs to shutdown. It
// must be called when finished after any successful Init ran.
func (obj *Lang) Close() error {
	var err error
	if obj.funcs != nil {
		err = obj.funcs.Cleanup()
	}
	close(obj.closeChan)
	obj.wg.Wait()
	return err
}

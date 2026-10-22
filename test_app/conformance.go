// The conformance run: every row of `test/fixtures/reference_topologies.txt`
// driven through THIS SDK's builder, so what is measured is what an author of
// a Go relay writes.
//
// Two entry points, both off main:
//
//	FLEZHA_CONFORMANCE_REPORT=<path>  drive every row and write the report the
//	                                  drill reads
//	                                  (test/system/topology_refusal_e2e_test.exs),
//	                                  then exit without connecting
//	FLEZHA_REFUSAL_FIXTURE=<id>       register ONLY that row's topology at
//	                                  startup, so a refused one kills the
//	                                  process with the refusal's own name in
//	                                  its output
//
// The palette tasklets exist only for this run: nothing registers them but a
// row that names them.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	flezha "github.com/flezha/flezha_go_sdk"
	"github.com/flezha/flezha_go_sdk/internal/check"
	"github.com/flezha/flezha_go_sdk/internal/contract"
)

const conformanceFixture = "test/fixtures/reference_topologies.txt"

// conformanceSurfaces are the authoring surfaces this builder carries. A row
// needing anything else is skipped BY NAME, which is what makes the skip
// checkable. Go cannot write `detached`: a node exists only once something
// wires it.
var conformanceSurfaces = []string{
	"branch", "merge", "steps", "shorthand", "shorthand_typed_output", "call", "untyped_tree", "schema",
}

// citedConf are the topologies the fixture cites rather than re-spells: this
// app's own fixtures, by the factory id each registers under.
var citedConf = map[string]string{
	"hybrid_channel":     "hybrid_channel",
	"commit_block_chain": "tier_commit_block_steps",
}

// -- the palette ---------------------------------------------------------------

// paletteConf is what every palette role shares: a pass-through Run and the
// mailbox capacity a row may declare on the role.
type paletteConf struct{ mailbox *int }

func (p paletteConf) spec(s flezha.Spec) flezha.Spec {
	s.MailboxCapacity = p.mailbox
	return s
}

func (paletteConf) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

type plainConf struct{ paletteConf }

func (p plainConf) Spec() flezha.Spec { return p.spec(flezha.Spec{Name: "plain"}) }

type secondConf struct{ paletteConf }

func (p secondConf) Spec() flezha.Spec { return p.spec(flezha.Spec{Name: "second"}) }

// twinConf is a distinct type declaring the SAME name as plain: that IS the collision.
type twinConf struct{ paletteConf }

func (p twinConf) Spec() flezha.Spec { return p.spec(flezha.Spec{Name: "plain"}) }

type staticConf struct{ paletteConf }

func (p staticConf) Spec() flezha.Spec {
	return p.spec(flezha.Spec{Name: "static", CellKeys: []string{"a", "b"}})
}

type routedConf struct{ paletteConf }

func (p routedConf) Spec() flezha.Spec {
	return p.spec(flezha.Spec{Name: "routed", CellKeys: []string{"a", "b"}})
}

func (routedConf) CellsFor(string) []string { return []string{"a"} }

type dynamicConf struct{ paletteConf }

func (p dynamicConf) Spec() flezha.Spec { return p.spec(flezha.Spec{Name: "dynamic"}) }

type noChangeConf struct{ paletteConf }

func (p noChangeConf) Spec() flezha.Spec {
	return p.spec(flezha.Spec{Name: "nochange", LogicalClock: true})
}

type eventTimeConf struct{ paletteConf }

func (p eventTimeConf) Spec() flezha.Spec { return p.spec(flezha.Spec{Name: "eventtime"}) }

func (eventTimeConf) EventTime(in string) uint64 { return uint64(len(in)) }

type paletteTaskletConf = flezha.Tasklet[string, flezha.NoState, string]

// -- a row's relay -------------------------------------------------------------

// rowConf is one line of the refusal table.
type rowConf struct {
	refusal, topology, steps, needs string
}

// rowRelayConf is a row's steps as a relay, written the way an author writes one.
type rowRelayConf struct {
	factoryID    string
	steps        []string
	run, durable any
	// Read before the steps run: a tasklet is bound by the `into` that first
	// names it, one step earlier than the `mailbox` step that declares its
	// capacity, and an entry takes its schema as it is declared.
	mailbox map[string]int
	schemas map[string]any
}

func newRowRelayConf(row rowConf) *rowRelayConf {
	r := &rowRelayConf{factoryID: row.topology, mailbox: map[string]int{}, schemas: map[string]any{}}
	for _, step := range strings.Split(row.steps, ";") {
		if step = strings.TrimSpace(step); step != "" {
			r.steps = append(r.steps, step)
		}
	}
	for _, step := range r.steps {
		head, rest, found := strings.Cut(step, ".")
		if !found {
			continue
		}
		verb, arg := splitCallConf(rest)
		switch {
		case head == "class" && verb == "run":
			r.run = decodeJSONConf(arg)
		case head == "class" && verb == "durable":
			r.durable = decodeJSONConf(arg)
		case head == "class":
			panic(fmt.Sprintf("the fixture spells a class setting this interpreter does not map: %s", verb))
		case verb == "mailbox":
			r.mailbox[head] = atoiConf(arg)
		case verb == "schema":
			r.schemas[head] = decodeJSONConf(arg)
		}
	}
	return r
}

func (r *rowRelayConf) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{FactoryID: r.factoryID, Run: r.run, Durable: r.durable}
}

func (r *rowRelayConf) Channels(c *flezha.ChannelBuilder) {
	d := &declareConf{row: r, c: c, handles: map[string]flezha.Stream[string]{}}
	for _, step := range r.steps {
		d.step(step)
	}
}

// declareConf is one declaration of a row: a handle is a channel id from an
// `entry(...)` step, or a palette role naming that tasklet's output channel.
type declareConf struct {
	row     *rowRelayConf
	c       *flezha.ChannelBuilder
	handles map[string]flezha.Stream[string]
}

func (d *declareConf) step(step string) {
	verb, arg := splitCallConf(step)
	switch verb {
	case "entry":
		d.handles[arg] = flezha.Input[string](d.c, arg, flezha.InputOptions{Schema: d.row.schemas[arg]})
	case "branch":
		d.branch(arg)
	case "merge":
		d.merge(arg)
	default:
		d.dotted(step)
	}
}

// dotted is `<handle>.<link>…`: a chain declared off a handle, a channel knob
// or a tasklet knob. A class setting was read as the row was loaded.
func (d *declareConf) dotted(step string) {
	head, rest, found := strings.Cut(step, ".")
	if !found {
		panic(fmt.Sprintf("a step is a call or a chain: read `%s`", step))
	}
	if head == "class" {
		return
	}
	edge := d.handle(head)
	for _, link := range strings.Split(rest, ".") {
		edge = d.link(edge, link, head)
	}
}

func (d *declareConf) link(edge flezha.Stream[string], link, role string) flezha.Stream[string] {
	verb, arg := splitCallConf(link)
	if strings.HasPrefix(verb, "call") {
		return d.call(edge, verb, arg, role)
	}
	switch verb {
	case "map":
		return flezha.Map(edge, func(v string) string { return v })
	case "filter":
		return edge.Filter(func(string) bool { return true })
	case "flat_map":
		return flezha.FlatMap(edge, func(v string) []string { return []string{v} })
	case "key":
		return edge.KeyBy(func(v string) string { return v })
	case "named":
		return edge.Named(arg)
	case "keyed":
		return edge.Keyed()
	case "retry_max":
		return edge.RetryMax(atoiConf(arg))
	case "on_full":
		return edge.OnFull(overflowConf(arg))
	case "output":
		return edge.Output(arg)
	case "into":
		d.handles[arg] = flezha.Into(edge, d.tasklet(arg))
		return d.handles[arg]
	case "mailbox", "schema":
		// Read as the row was loaded.
		return edge
	}
	panic(fmt.Sprintf("the fixture spells a link this interpreter does not map: %s", verb))
}

// call is `<x>.call…(<arg>)`: a 500 ms call whose Response channel feeds the
// role that produces x, unless the step leaves it unread.
func (d *declareConf) call(edge flezha.Stream[string], verb, arg, role string) flezha.Stream[string] {
	spec := flezha.CallSpec{DeadlineMs: 500}
	switch verb {
	case "call", "call_unread":
		spec.Target = arg
	case "call_factory":
		spec.Factory = arg
	case "call_factory_nocreate":
		spec.Factory, spec.NoCreate = arg, true
	case "call_deadline":
		spec.Target, spec.DeadlineMs = "pricing", int64(atoiConf(arg))
	default:
		panic(fmt.Sprintf("the fixture spells a call this interpreter does not map: %s", verb))
	}
	response := flezha.Call[string, string](edge, spec)
	if verb != "call_unread" {
		flezha.Into(flezha.Map(response, renderCall), d.tasklet(role))
	}
	return edge
}

// merge is `merge(<role>, <x>, <x>…)`: one merge over the listed sources, each
// mapped as it stands.
func (d *declareConf) merge(arg string) {
	parts := splitListConf(arg)
	m := flezha.NewMerge[string](d.c)
	for _, source := range parts[1:] {
		flezha.From(m, d.handle(source), func(v string) string { return v })
	}
	d.handles[parts[0]] = flezha.MergeInto(m, d.tasklet(parts[0]))
}

// branch is `branch(<x>, <n>)`, the branch sugar's arity. The arms are empty:
// what the row states is how many there are.
func (d *declareConf) branch(arg string) {
	parts := splitListConf(arg)
	arms := make([]func(flezha.Stream[string]), atoiConf(parts[1]))
	for i := range arms {
		arms[i] = func(flezha.Stream[string]) {}
	}
	d.handle(parts[0]).Branch(arms...)
}

func (d *declareConf) handle(name string) flezha.Stream[string] {
	h, ok := d.handles[name]
	if !ok {
		panic(fmt.Sprintf("the row addresses `%s`, which nothing has declared", name))
	}
	return h
}

// tasklet is the palette role bound to its fixture tasklet, carrying the
// mailbox capacity the row declares on it.
func (d *declareConf) tasklet(role string) paletteTaskletConf {
	var p paletteConf
	if capacity, ok := d.row.mailbox[role]; ok {
		p.mailbox = &capacity
	}
	switch role {
	case "plain":
		return plainConf{p}
	case "second":
		return secondConf{p}
	case "twin":
		return twinConf{p}
	case "static":
		return staticConf{p}
	case "routed":
		return routedConf{p}
	case "dynamic":
		return dynamicConf{p}
	case "nochange":
		return noChangeConf{p}
	case "eventtime":
		return eventTimeConf{p}
	}
	panic(fmt.Sprintf("the fixture names a palette role that is not one: %s", role))
}

func overflowConf(policy string) flezha.Overflow {
	switch p := flezha.Overflow(policy); p {
	case flezha.DropNothing, flezha.DropNewest, flezha.DropOldest:
		return p
	}
	panic(fmt.Sprintf("the fixture spells an overflow policy this interpreter does not map: %s", policy))
}

// -- the short-form palette ----------------------------------------------------

type shortBodyConf struct{}

func (shortBodyConf) Spec() flezha.Spec { return flezha.Spec{} }

func (shortBodyConf) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

// shortTopologyConf is a short form's tasklet that also carries a topology body.
type shortTopologyConf struct{ shortBodyConf }

func (shortTopologyConf) Channels(c *flezha.ChannelBuilder) {
	flezha.Into(flezha.Input[string](c, "input"), plainConf{}).Output("out")
}

// shortNoOutConf is a short form's tasklet declaring no output.
type shortNoOutConf struct{}

func (shortNoOutConf) Spec() flezha.Spec { return flezha.Spec{} }

func (shortNoOutConf) Run(string, *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error { return nil }

// shorthandConf is a row's `shorthand(<id>|-, <output>|-, <input>|-)` steps,
// each as the short form it describes. A row's first is a flow relay and its
// second runs in steps, so a collision crosses runs.
func shorthandConf(row rowConf) []flezha.Relay {
	var relays []flezha.Relay
	for _, step := range strings.Split(row.steps, ";") {
		step = strings.TrimSpace(step)
		body, modifier := step, ""
		for _, suffix := range []string{".noout", ".topology"} {
			if trimmed, found := strings.CutSuffix(step, suffix); found {
				body, modifier = trimmed, suffix
			}
		}
		verb, args := splitCallConf(body)
		if verb != "shorthand" {
			continue
		}
		fields := splitListConf(args)
		if len(fields) != 3 {
			panic(fmt.Sprintf("a shorthand step declares three fields: read `%s`", step))
		}
		for i, field := range fields {
			if field == "-" {
				fields[i] = ""
			}
		}
		opts := flezha.ShortOptions{Output: fields[1], Input: fields[2]}
		if len(relays) > 0 {
			opts.Run = "steps"
		}
		switch modifier {
		case ".noout":
			relays = append(relays, flezha.ShortTasklet(fields[0], shortNoOutConf{}, opts))
		case ".topology":
			relays = append(relays, flezha.ShortTasklet(fields[0], shortTopologyConf{}, opts))
		default:
			relays = append(relays, flezha.ShortTasklet(fields[0], shortBodyConf{}, opts))
		}
	}
	return relays
}

// -- the report ----------------------------------------------------------------

type reportRowConf struct {
	Topology string               `json:"topology"`
	Outcome  string               `json:"outcome"`
	Names    []string             `json:"names,omitempty"`
	Needs    string               `json:"needs,omitempty"`
	Derived  []derivedChannelConf `json:"derived,omitempty"`
}

// derivedChannelConf is a derived channel in the columns the fixture's
// seven-column table states: the consumers as the flat list of tasklet indices.
type derivedChannelConf struct {
	Index     int     `json:"index"`
	Name      string  `json:"name"`
	Kind      string  `json:"kind"`
	Producer  *int    `json:"producer"`
	Consumers []int   `json:"consumers"`
	Sink      *string `json:"sink"`
}

// writeReport drives the whole table and writes the report. Nothing here
// connects to a controller: the run is the builder's answer to a file.
func writeReport(path string) (err error) {
	defer recoverConf(&err)
	rows, err := rowsConf()
	if err != nil {
		return err
	}
	judged := make([]reportRowConf, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		entry, err := judgeConf(row)
		if err != nil {
			return fmt.Errorf("%s: %w", row.topology, err)
		}
		if entry.Outcome == "skipped" {
			skipped++
		}
		judged = append(judged, entry)
	}
	report, err := json.Marshal(map[string]any{"runtime": "go", "rows": judged})
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, report, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "conformance: %d rows, %d ran, %d skipped\n", len(rows), len(rows)-skipped, skipped)
	return nil
}

// judgeConf is one row's verdict. The refusal names are the core's own (or
// the SDK's for a short form), carried on the refusal; nothing here
// re-derives one.
func judgeConf(row rowConf) (reportRowConf, error) {
	entry := reportRowConf{Topology: row.topology}
	if !surfaceCarriedConf(row) {
		entry.Outcome, entry.Needs = "skipped", row.needs
		return entry, nil
	}
	refused := func(err error) (reportRowConf, error) {
		var refusal *flezha.Error
		if !errors.As(err, &refusal) {
			return entry, err
		}
		entry.Outcome, entry.Names = "refused", codesOf(refusal)
		return entry, nil
	}
	if shorthand := shorthandConf(row); len(shorthand) > 0 {
		// A short form is refused at registration, and a duplicate factory id
		// by the registry: a real runner is driven, never started.
		runner, err := flezha.CreateRunner("conformance_shorthand")
		if err != nil {
			return entry, err
		}
		for _, relay := range shorthand {
			if err := runner.RegisterFactory(relay); err != nil {
				return refused(err)
			}
		}
		entry.Outcome = "accepted"
		return entry, nil
	}
	relay, err := relayForConf(row)
	if err != nil {
		return entry, err
	}
	answer, err := check.Derived(relay)
	if err != nil {
		return refused(err)
	}
	if entry.Derived, err = channelsOfConf(answer); err != nil {
		return entry, err
	}
	entry.Outcome = "accepted"
	return entry, nil
}

// channelsOfConf is the core's derived declaration in the report's columns.
func channelsOfConf(answer []byte) ([]derivedChannelConf, error) {
	var derived struct {
		Channels []struct {
			Index     int     `json:"index"`
			Name      string  `json:"name"`
			Kind      string  `json:"kind"`
			Producer  *int    `json:"producer"`
			Sink      *string `json:"sink"`
			Consumers []struct {
				Tasklet int `json:"tasklet"`
			} `json:"consumers"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(answer, &derived); err != nil {
		return nil, err
	}
	channels := make([]derivedChannelConf, 0, len(derived.Channels))
	for _, ch := range derived.Channels {
		consumers := make([]int, 0, len(ch.Consumers))
		for _, edge := range ch.Consumers {
			consumers = append(consumers, edge.Tasklet)
		}
		channels = append(channels, derivedChannelConf{
			Index: ch.Index, Name: ch.Name, Kind: ch.Kind, Producer: ch.Producer, Consumers: consumers, Sink: ch.Sink,
		})
	}
	return channels, nil
}

// codesOf is every code a refusal carries: each issue's, or its own.
func codesOf(refused *flezha.Error) []string {
	issues, _ := refused.Details["issues"].([]any)
	var codes []string
	for _, issue := range issues {
		if fields, ok := issue.(map[string]any); ok {
			if code, ok := fields["code"].(string); ok {
				codes = append(codes, code)
			}
		}
	}
	if len(codes) == 0 {
		return []string{refused.Code}
	}
	return codes
}

// registerOnly registers ONLY the named row's topology, the way the app
// registers any relay. A refused row never reaches a connected runner: the
// declaration is judged before the core is started, and the process dies
// naming the refusal.
func registerOnly(runner *flezha.Runner, topology string) (err error) {
	defer recoverConf(&err)
	rows, err := rowsConf()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.topology != topology {
			continue
		}
		if !surfaceCarriedConf(row) {
			return fmt.Errorf("%s: this SDK's builder carries no `%s`, so the row is skipped and nothing is registered", topology, row.needs)
		}
		if shorthand := shorthandConf(row); len(shorthand) > 0 {
			for _, relay := range shorthand {
				if err := runner.RegisterFactory(relay); err != nil {
					return err
				}
			}
			return nil
		}
		relay, err := relayForConf(row)
		if err != nil {
			return err
		}
		return runner.RegisterFactory(relay)
	}
	return fmt.Errorf("%s has no row `%s`", conformanceFixture, topology)
}

func surfaceCarriedConf(row rowConf) bool {
	return row.needs == "-" || slices.Contains(conformanceSurfaces, row.needs)
}

// relayForConf is a row's relay: one of this app's own fixtures for a cited
// topology, else the row's steps.
func relayForConf(row rowConf) (flezha.Relay, error) {
	factoryID, cited := citedConf[row.topology]
	if !cited {
		return newRowRelayConf(row), nil
	}
	for _, relay := range allRelays() {
		if relay.Spec().FactoryID == factoryID {
			return relay, nil
		}
	}
	return nil, fmt.Errorf("the fixture cites %s, which this app does not declare as %s", row.topology, factoryID)
}

// rowsConf is the refusal table, in file order.
func rowsConf() ([]rowConf, error) {
	body, err := contract.Read(conformanceFixture)
	if err != nil {
		return nil, err
	}
	var rows []rowConf
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := splitListOnConf(line, "|")
		if len(cols) == 4 {
			rows = append(rows, rowConf{refusal: cols[0], topology: cols[1], steps: cols[2], needs: cols[3]})
		}
	}
	return rows, nil
}

// -- spelling helpers ----------------------------------------------------------

// splitCallConf is `verb(arg)` as ("verb", "arg"), and a bare word as ("word", "").
func splitCallConf(text string) (string, string) {
	verb, rest, found := strings.Cut(text, "(")
	if !found {
		return strings.TrimSpace(text), ""
	}
	return strings.TrimSpace(verb), strings.TrimSpace(strings.TrimRight(rest, ")"))
}

func splitListConf(text string) []string { return splitListOnConf(text, ",") }

func splitListOnConf(text, sep string) []string {
	parts := strings.Split(text, sep)
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}
	return parts
}

func atoiConf(text string) int {
	n, err := strconv.Atoi(text)
	if err != nil {
		panic(fmt.Sprintf("the fixture spells a number this interpreter cannot read: %q", text))
	}
	return n
}

// decodeJSONConf is a step's JSON argument as data, its numbers kept as written.
func decodeJSONConf(text string) any {
	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		panic(fmt.Sprintf("the fixture's JSON does not decode: %s: %v", text, err))
	}
	return value
}

// recoverConf turns the interpreter's panic on a spelling it does not map into
// the run's error.
func recoverConf(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("conformance: %v", r)
	}
}

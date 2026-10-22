package flezha

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// Version is this SDK's own released version, stamped into every durable
// record it writes. Go modules carry no version in go.mod, so this constant is
// the one source of truth and the release checklist bumps it.
const Version = "0.1.0"

// sdkLangGo is the Go SDK's byte in the core's sdk_lang allocation table.
const sdkLangGo = 8

// formatIDJSON names the record body's codec, JSON; sdk_lang keeps the
// languages that share it apart.
const formatIDJSON = 6

// classSpec is one declared relay: its settings and its graph.
type classSpec struct {
	relay    RelaySpec
	tasklets []*taskNode
	channels []*channelSpec
	steps    bool
}

// specOf builds a relay's graph and reads its settings.
func specOf(r Relay) (*classSpec, error) {
	settings := r.Spec()
	if settings.FactoryID == "" {
		return nil, refusal("topology.missing_factory_id", map[string]any{"relay": typeName(reflect.TypeOf(r))})
	}
	b := newBuilder(settings.FactoryID)
	r.Channels(b)
	b.seal()
	if b.err != nil {
		return nil, b.err
	}
	return &classSpec{relay: settings, tasklets: b.tasklets, channels: b.channels, steps: branchOf(settings.Run) == "steps"}, nil
}

// branchOf is the branch a settings tree names: its bare name, or its one key.
func branchOf(tree any) string {
	switch t := tree.(type) {
	case string:
		return t
	case map[string]any:
		if len(t) == 1 {
			for k := range t {
				return k
			}
		}
	}
	return ""
}

// The declaration the core deserializes, every key pinned by
// `test/fixtures/host_class_decl_keys.txt`. A nil field is left out, so the
// core's default applies.
type classDecl struct {
	FactoryID         string        `json:"factory_id"`
	Description       *string       `json:"description,omitempty"`
	MaxDurationMs     *int64        `json:"max_duration_ms,omitempty"`
	Run               any           `json:"run,omitempty"`
	Durable           any           `json:"durable,omitempty"`
	SdkLang           int           `json:"sdk_lang"`
	SdkVersion        string        `json:"sdk_version"`
	ChangeFormatID    int           `json:"change_format_id"`
	CompactAfterBytes *int64        `json:"compact_after_bytes,omitempty"`
	Channels          []channelDecl `json:"channels"`
	Tasklets          []taskletDecl `json:"tasklets"`
}

type channelDecl struct {
	Name           string             `json:"name"`
	Kind           string             `json:"kind"`
	Producer       *int               `json:"producer,omitempty"`
	Chains         [][]map[string]any `json:"chains"`
	RetryMax       *int               `json:"retry_max,omitempty"`
	RetryBackoffMs *int               `json:"retry_backoff_ms,omitempty"`
	Overflow       Overflow           `json:"overflow"`
	Keyed          bool               `json:"keyed"`
	Codec          *string            `json:"codec,omitempty"`
	Schema         any                `json:"schema,omitempty"`
	Description    *string            `json:"description,omitempty"`
}

type taskletDecl struct {
	Name                     string   `json:"name"`
	CellKeys                 []string `json:"cell_keys"`
	IdleTTLMs                *int64   `json:"idle_ttl_ms,omitempty"`
	MaxCells                 *int     `json:"max_cells,omitempty"`
	CellsForOverridden       bool     `json:"cells_for_overridden"`
	CompensateOverridden     bool     `json:"compensate_overridden"`
	Clock                    string   `json:"clock"`
	MailboxCapacity          *int     `json:"mailbox_capacity,omitempty"`
	ChangeIsNochange         bool     `json:"change_is_nochange"`
	SchemaVersion            uint32   `json:"schema_version"`
	MinReadableSchemaVersion *uint32  `json:"min_readable_schema_version,omitempty"`
	Migrates                 bool     `json:"migrates"`
	MaxTimersPerCell         *int     `json:"max_timers_per_cell,omitempty"`
}

// declaration is the class's record, as JSON.
func (spec *classSpec) declaration() []byte {
	r := spec.relay
	decl := classDecl{
		FactoryID: r.FactoryID, Run: r.Run, Durable: r.Durable,
		SdkLang: sdkLangGo, SdkVersion: Version, ChangeFormatID: formatIDJSON,
		Channels: []channelDecl{}, Tasklets: []taskletDecl{},
		Description: nonZero(r.Description), MaxDurationMs: nonZero(r.MaxDurationMs),
	}
	for _, node := range spec.tasklets {
		if bytes := node.def.spec.CompactAfterBytes; bytes > 0 && (decl.CompactAfterBytes == nil || bytes < *decl.CompactAfterBytes) {
			decl.CompactAfterBytes = &bytes
		}
		decl.Tasklets = append(decl.Tasklets, taskletOf(node))
	}
	for _, ch := range spec.channels {
		decl.Channels = append(decl.Channels, spec.channelOf(ch))
	}
	out, err := json.Marshal(decl)
	if err != nil {
		// Every field is a plain value or a tree the author wrote as JSON data.
		panic("flezha: the declaration does not encode: " + err.Error())
	}
	return out
}

func (spec *classSpec) channelOf(ch *channelSpec) channelDecl {
	chains := ch.chains
	if chains == nil {
		chains = [][]map[string]any{}
	}
	decl := channelDecl{
		Name: ch.name, Kind: ch.kind, Producer: ch.producer, Chains: chains,
		RetryMax: ch.retryMax, RetryBackoffMs: ch.retryBackoffMs, Overflow: ch.overflow, Keyed: ch.keyed,
		Schema: ch.schema, Codec: nonZero(ch.codec), Description: nonZero(ch.description),
	}
	if decl.Schema == nil && ch.codec == string(JSON) {
		if t := spec.valueType(ch); t != nil {
			if schema := schemaOf(t); schema != nil {
				decl.Schema = schema
			}
		}
	}
	return decl
}

// valueType is the Go type a channel's API values have, where the graph names
// it: an entry's is the input of the tasklet a chain wires it into with no
// transform, an output's the output of the tasklet producing it.
func (spec *classSpec) valueType(ch *channelSpec) reflect.Type {
	if ch.kind == "entry" {
		for _, links := range ch.chains {
			var first map[string]any
			for _, link := range links {
				if link["link"] != "key" {
					first = link
					break
				}
			}
			if first != nil && first["link"] == "into" {
				return spec.tasklets[first["tasklet"].(int)].def.inType
			}
		}
		return nil
	}
	if ch.sink != nil && ch.call == nil && ch.producer != nil {
		return spec.tasklets[*ch.producer].def.outType
	}
	return nil
}

func taskletOf(node *taskNode) taskletDecl {
	d := node.def
	s := d.spec
	cellKeys := s.CellKeys
	if cellKeys == nil {
		cellKeys = []string{}
	}
	schemaVersion := s.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = 1
	}
	decl := taskletDecl{
		Name: node.name, CellKeys: cellKeys, CellsForOverridden: d.hasCellsFor,
		CompensateOverridden: d.hasCompensate, Clock: d.clock(), MailboxCapacity: s.MailboxCapacity,
		ChangeIsNochange: !d.journals, SchemaVersion: schemaVersion, Migrates: d.migrates,
		IdleTTLMs: nonZero(max(s.IdleTTLSeconds, 0) * 1000), MaxCells: nonZero(s.MaxCells),
		MinReadableSchemaVersion: nonZero(s.MinReadableSchemaVersion), MaxTimersPerCell: nonZero(s.MaxTimersPerCell),
	}
	return decl
}

// nonZero is v's address, or nil for the zero value: a setting left out of the
// declaration, so the core's default applies.
func nonZero[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

// derived is what flz_core_check_class answers: the consumers and kinds the
// core read off the record's chains.
type derived struct {
	Channels []struct {
		Index     int    `json:"index"`
		Kind      string `json:"kind"`
		Consumers []struct {
			Tasklet int `json:"tasklet"`
			Chain   int `json:"chain"`
		} `json:"consumers"`
	} `json:"channels"`
}

// check has the core judge the declaration, and binds the edges it derived to
// the closures the builder held aside for each chain: which edge survives a
// re-declaration is the core's answer.
func (spec *classSpec) check(ctx *native.Context) error {
	answer, err := ctx.CheckClass(spec.declaration())
	if err != nil {
		return err
	}
	var d derived
	if err := json.Unmarshal(answer, &d); err != nil {
		return err
	}
	for _, ch := range d.Channels {
		target := spec.channels[ch.Index]
		target.consumers = target.consumers[:0]
		for _, edge := range ch.Consumers {
			target.consumers = append(target.consumers, target.chainEdges[edge.Chain])
		}
		target.derivedKind = ch.Kind
	}
	return nil
}

// schemaOf is the JSON Schema a struct type's exported fields give, for a
// relay's API; nil for any other type, so derivation never refuses a type. A
// field with omitempty or a pointer type is not required.
func schemaOf(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	schema, _ := objectSchema(t, map[reflect.Type]bool{}).(map[string]any)
	if props, _ := schema["properties"].(map[string]any); len(props) == 0 {
		return nil
	}
	return schema
}

func objectSchema(t reflect.Type, open map[reflect.Type]bool) any {
	if open[t] {
		return map[string]any{}
	}
	open[t] = true
	defer delete(open, t)
	properties := map[string]any{}
	var required []string
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, omitempty := jsonName(f)
		if name == "-" {
			continue
		}
		properties[name] = typeSchema(f.Type, open)
		if !omitempty && f.Type.Kind() != reflect.Pointer {
			required = append(required, name)
		}
	}
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func typeSchema(t reflect.Type, open map[reflect.Type]bool) any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Struct:
		return objectSchema(t, open)
	default:
		return map[string]any{}
	}
}

// jsonName is a field's name as encoding/json writes it, and whether it is omitempty.
func jsonName(f reflect.StructField) (string, bool) {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name, false
	}
	name, opts, _ := strings.Cut(tag, ",")
	if name == "" {
		name = f.Name
	}
	return name, slices.Contains(strings.Split(opts, ","), "omitempty")
}

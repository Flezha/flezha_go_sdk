package flezha

import (
	"encoding/json"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// Control administers the namespace this runner's connection proved, from
// ordinary application code: start and stop relays, enable and bind plugins,
// read what is running. The controller judges every call as the identity the
// connection established, so the application holds no credential; a user
// grants a namespace's applications the authority once:
//
//	flz role bind runner@team-orders app team-orders
type Control struct {
	core *native.Context
}

// StartResult is the relay a start reached, and whether it started it.
type StartResult struct {
	ID      string `json:"id"`
	Outcome string `json:"outcome"`
}

// StartRelay starts a relay of factoryID. Without an id the controller
// generates one, so a start retried after a lost answer starts a second relay;
// with one, a start of an id already running answers already_running.
// initValues and opts are the specification `flz relay start` takes, nil for none.
func (c *Control) StartRelay(factoryID, id string, initValues, opts any) (StartResult, error) {
	var result StartResult
	spec := given(map[string]any{"id": id, "init_values": initValues, "opts": opts})
	spec["factory_id"] = factoryID
	answer, err := c.Call("Relay.start", spec)
	if err != nil {
		return result, err
	}
	err = remarshal(answer, &result)
	if err == nil && (result.ID == "" || result.Outcome == "") {
		err = refusal("control.unreadable_answer", map[string]any{"verb": "Relay.start"})
	}
	return result, err
}

// StopRelay stops a relay, or the whole partition group it names.
func (c *Control) StopRelay(relayID string) (any, error) { return c.Call("Relay.stop", relayID) }

// EnablePlugin enables the plugin name of provider (for example
// relay_websocket) on opts. single runs one copy in the whole cluster;
// relayID with hooks binds that relay in the same call.
func (c *Control) EnablePlugin(name, provider string, opts map[string]any, single bool, relayID string, hooks []any) (any, error) {
	if opts == nil {
		opts = map[string]any{}
	}
	spec := given(map[string]any{"single": single, "relay_id": relayID, "hooks": hooks})
	spec["provider"], spec["opts"] = provider, opts
	return c.Call("PluginManager.enable", name, spec)
}

// BindPlugin binds relayID to the enabled plugin name, with optional hooks and channel.
func (c *Control) BindPlugin(name, relayID string, hooks []any, channel string) (any, error) {
	spec := given(map[string]any{"hooks": hooks, "channel": channel})
	spec["relay_id"] = relayID
	return c.Call("PluginManager.bind", name, spec)
}

// UnbindPlugin unbinds relayID from the plugin name, which stays enabled.
func (c *Control) UnbindPlugin(name, relayID string) (any, error) {
	return c.Call("PluginManager.unbind", name, map[string]any{"relay_id": relayID})
}

// DisablePlugin disables the plugin name, stopping it wherever it runs.
func (c *Control) DisablePlugin(name string) (any, error) {
	return c.Call("PluginManager.disable", name, map[string]any{})
}

// ListRelays is the relays in this namespace, one row each.
func (c *Control) ListRelays() (any, error) { return c.Call("Relay.list") }

// ListPlugins is the plugins enabled in this namespace, one row each.
func (c *Control) ListPlugins() (any, error) { return c.Call("PluginManager.enabled") }

// CellState is a relay's live state and the change sequence it was read at.
// cell is `<tasklet>[/<cell_key>]`; "" reads every cell of the relay.
func (c *Control) CellState(relayID, cell string) (any, error) {
	if cell == "" {
		return c.Call("Relay.cell_state", relayID, nil)
	}
	return c.Call("Relay.cell_state", relayID, cell)
}

// Call is any published function, by name, with its arguments other than the
// namespace, which the controller fills from the connection.
func (c *Control) Call(verb string, args ...any) (any, error) {
	if args == nil {
		args = []any{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	answer, err := c.core.AdminCall(verb, encoded)
	if err != nil {
		return nil, err
	}
	var out any
	err = json.Unmarshal(answer, &out)
	return out, err
}

// given is a call's named parameters without the ones it was not given: a
// nil, an empty string or false.
func given(fields map[string]any) map[string]any {
	for key, value := range fields {
		switch v := value.(type) {
		case nil:
			delete(fields, key)
		case string:
			if v == "" {
				delete(fields, key)
			}
		case bool:
			if !v {
				delete(fields, key)
			}
		case []any:
			if v == nil {
				delete(fields, key)
			}
		}
	}
	return fields
}

func remarshal(from, into any) error {
	b, err := json.Marshal(from)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, into)
}

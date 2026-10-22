package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

// localSendConf is one entry of FLEZHA_LOCAL_SEND_SCRIPT: a local send of
// message, or with call a call of the channel with value through the
// controller, made once the file when names exists.
type localSendConf struct {
	Relay   string `json:"relay"`
	Channel string `json:"channel"`
	Message string `json:"message"`
	When    string `json:"when"`
	Call    bool   `json:"call"`
	Value   any    `json:"value"`
}

// localSendDrill makes the drill's sends from this app's own runner, beside
// it: the test creates each trigger file only once the runner serves, so a
// send never races the bind. Each prints `local_send <relay> ok | err <code>`
// or `call <relay> ok <answer JSON> | err <code>`.
func localSendDrill(runner *flezha.Runner, script string) {
	decoder := json.NewDecoder(bytes.NewReader([]byte(script)))
	// A number is sent as it was written.
	decoder.UseNumber()
	var sends []localSendConf
	if err := decoder.Decode(&sends); err != nil {
		fail(fmt.Errorf("FLEZHA_LOCAL_SEND_SCRIPT is a JSON array: %w", err))
	}
	go func() {
		for _, send := range sends {
			for {
				if _, err := os.Stat(send.When); err == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if send.Call {
				answer, err := runner.Call(send.Relay, send.Channel, send.Value)
				if err != nil {
					fmt.Printf("call %s err %s\n", send.Relay, codeOfConf(err))
					continue
				}
				encoded, err := json.Marshal(answer)
				if err != nil {
					fmt.Printf("call %s err %s\n", send.Relay, err)
					continue
				}
				fmt.Printf("call %s ok %s\n", send.Relay, encoded)
				continue
			}
			if _, err := runner.Send(send.Relay, send.Channel, []byte(send.Message), flezha.Local()); err != nil {
				fmt.Printf("local_send %s err %s\n", send.Relay, codeOfConf(err))
				continue
			}
			fmt.Printf("local_send %s ok\n", send.Relay)
		}
	}()
}

func codeOfConf(err error) string {
	var refused *flezha.Error
	if errors.As(err, &refused) {
		return refused.Code
	}
	return err.Error()
}

package command

import (
	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
)

func storeErr(err error) protocol.Value {
	if err == nil {
		return protocol.SimpleStringValue("OK")
	}
	if err == store.ErrOOM {
		return protocol.ErrorValue("OOM command not allowed when used memory > 'maxmemory'")
	}
	if err == store.ErrWrongType {
		return protocol.ErrorValue(err.Error())
	}
	// Integer parse style messages already include wording.
	msg := err.Error()
	if len(msg) >= 9 && msg[:9] == "WRONGTYPE" {
		return protocol.ErrorValue(msg)
	}
	return protocol.ErrorValue("ERR " + msg)
}

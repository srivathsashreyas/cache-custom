package command

import (
	"cache-custom/internal/persist"
	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

// RegisterPersist wires SAVE/BGSAVE/LASTSAVE/FLUSHDB (require AUTH).
func RegisterPersist(r *Registry, eng *persist.Engine, tenants *tenant.Registry) {
	r.Register("SAVE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 1 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'save' command")
		}
		if ten, errv := requireDB(ctx); ten == nil {
			return errv
		}
		if eng == nil || eng.Mode() == persist.ModeNone {
			return protocol.ErrorValue("ERR Persistence disabled")
		}
		if err := eng.SaveSnapshot(tenants); err != nil {
			return protocol.ErrorValue("ERR " + err.Error())
		}
		return protocol.SimpleStringValue("OK")
	})

	r.Register("BGSAVE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 1 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'bgsave' command")
		}
		if ten, errv := requireDB(ctx); ten == nil {
			return errv
		}
		if eng == nil || eng.Mode() == persist.ModeNone {
			return protocol.ErrorValue("ERR Persistence disabled")
		}
		if err := eng.BGSave(tenants); err != nil {
			return protocol.ErrorValue(err.Error())
		}
		return protocol.SimpleStringValue("Background saving started")
	})

	r.Register("LASTSAVE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 1 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'lastsave' command")
		}
		if ten, errv := requireDB(ctx); ten == nil {
			return errv
		}
		if eng == nil {
			return protocol.IntegerValue(0)
		}
		return protocol.IntegerValue(eng.LastSaveUnix())
	})

	r.Register("FLUSHDB", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 1 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'flushdb' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		ten.DB.FlushDB()
		return protocol.SimpleStringValue("OK")
	})
}

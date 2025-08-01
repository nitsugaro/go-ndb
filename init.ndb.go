package ndb

import (
	goconf "github.com/nitsugaro/go-conf"
	"github.com/nitsugaro/go-ndb/cache"
)

func init() {
	goconf.OnLoad(func() {
		cache.SetCacheLimit(goconf.GetOpField("ndb.schema.cache_regex_limit", 100))
	})
}

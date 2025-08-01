package cache

import (
	"regexp"

	lru "github.com/hashicorp/golang-lru"
)

var regexCache *lru.Cache

func SetCacheLimit(limit int) {
	cache, err := lru.New(limit)
	if err != nil {
		panic(err)
	}

	regexCache = cache
}

func GetRegexp(pattern string) (*regexp.Regexp, error) {
	if val, ok := regexCache.Get(pattern); ok {
		return val.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	regexCache.Add(pattern, re)
	return re, nil
}

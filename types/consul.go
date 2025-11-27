package types

import (
	consulAPI "github.com/hashicorp/consul/api"
)

type ConsulKV interface {
	Get(key string, q *consulAPI.QueryOptions) (*consulAPI.KVPair, *consulAPI.QueryMeta, error)
	Put(p *consulAPI.KVPair, q *consulAPI.WriteOptions) (*consulAPI.WriteMeta, error)
	Delete(key string, w *consulAPI.WriteOptions) (*consulAPI.WriteMeta, error)
	DeleteTree(prefix string, w *consulAPI.WriteOptions) (*consulAPI.WriteMeta, error)
	Keys(prefix, separator string, q *consulAPI.QueryOptions) ([]string, *consulAPI.QueryMeta, error)
}

package hazelcast

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client"
)

type HazelCast struct{}

func (HazelCast) Connect(ctx context.Context, addrs []string) (*hazelcast.Client, error) {
	config := hazelcast.Config{}

	if len(addrs) == 0 {
		addrs = append(make([]string, 0), "localhost:5701")
	}
	config.Cluster.Network.SetAddresses(addrs...)
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	return client, err
}

func (HazelCast) GetMap(ctx context.Context, client *hazelcast.Client) (*hazelcast.Map, error) {
	testMap, err := client.GetMap(ctx, "test")
	return testMap, err
}

func (HazelCast) Set(ctx context.Context, testMap *hazelcast.Map, key string, value interface{}) error {
	err := testMap.Set(ctx, key, value)
	return err
}

func (HazelCast) Get(ctx context.Context, testMap *hazelcast.Map, key string) (interface{}, error) {
	value, err := testMap.Get(ctx, key)
	return value, err
}

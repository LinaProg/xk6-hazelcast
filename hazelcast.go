package hazelcast

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/hazelcast", new(HazelCast))
}

type HazelCast struct {
	ctx context.Context
}

func (hc *HazelCast) Connect(addrs []string) (*hazelcast.Client, error) {

	hc.ctx = context.TODO()
	config := hazelcast.Config{}

	if len(addrs) == 0 {
		addrs = append(make([]string, 0), "localhost:5701")
	}
	config.Cluster.Network.SetAddresses(addrs...)
	client, err := hazelcast.StartNewClientWithConfig(hc.ctx, config)

	return client, err
}

func (hc *HazelCast) GetMap(client *hazelcast.Client, name string) (*hazelcast.Map, error) {
	mapping, err := client.GetMap(hc.ctx, name)
	return mapping, err
}

func (hc *HazelCast) Set(testMap *hazelcast.Map, key string, value interface{}) error {
	err := testMap.Set(hc.ctx, key, value)
	return err
}

func (hc *HazelCast) Put(testMap *hazelcast.Map, key string, value interface{}) (interface{}, error) {
	value, err := testMap.Put(hc.ctx, key, value)
	return value, err
}

func (hc *HazelCast) Get(testMap *hazelcast.Map, key string) (interface{}, error) {
	value, err := testMap.Get(hc.ctx, key)
	return value, err
}

func (hc *HazelCast) Del(testMap *hazelcast.Map, key string) error {
	err := testMap.LoadAllReplacing(hc.ctx, key)
	return err
}

// Counter operations

func (hc *HazelCast) getPNCounter(client *hazelcast.Client, name string) (*hazelcast.PNCounter, error) {
	pNCounter, err := client.GetPNCounter(hc.ctx, name)
	return pNCounter, err
}

func (hc *HazelCast) Incr(client *hazelcast.Client, name string) error {
	pNCounter, err := hc.getPNCounter(client, name)
	if err != nil {
		return err
	}
	_, err = pNCounter.IncrementAndGet(hc.ctx)
	return err
}

func (hc *HazelCast) Decr(client *hazelcast.Client, name string) error {
	pNCounter, err := hc.getPNCounter(client, name)
	if err != nil {
		return err
	}
	_, err = pNCounter.DecrementAndGet(hc.ctx)
	return err

}

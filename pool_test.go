package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodePool_Get(t *testing.T) {
	pool := NewNodePool()
	node := pool.Get()
	node.Key = []byte("hello")
	require.Equal(t, node.Key, pool.nodes[node.poolId].Key)
	pool.Put(node)
	require.Equal(t, []byte(nil), pool.nodes[node.poolId].Key)
}

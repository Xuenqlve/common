package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

type RedisCluster struct {
	client *redis.ClusterClient
}

func CreateRedisClusterConnection(address, username, password string, isTls bool) (*RedisCluster, error) {
	r := &RedisCluster{}
	addrs := strings.Split(address, ";")
	opt := &redis.ClusterOptions{
		Addrs:           addrs,
		Username:        username,
		Password:        password,
		ReadOnly:        false,
		MaxRedirects:    600,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 1 * time.Second,
		ReadTimeout:     15 * time.Second,
		WriteTimeout:    15 * time.Second,
	}
	if isTls {
		opt.TLSConfig.InsecureSkipVerify = true
	}
	r.client = redis.NewClusterClient(opt)
	_, err := r.client.Ping(context.Background()).Result()
	return r, err
}

func (r *RedisCluster) Writer(ctx context.Context, args ...any) error {
	_, err := r.client.Do(ctx, args...).Result()
	return err
}

func (r *RedisCluster) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

func (r *RedisCluster) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *RedisCluster) Close() error {
	return r.client.Close()
}

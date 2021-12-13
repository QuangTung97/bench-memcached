package main

import (
	"context"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

func benchMemcachedSet() {
	mc := memcache.New("localhost:11211")

	var wg sync.WaitGroup
	const numThreads = 4
	wg.Add(numThreads)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			for i := startIndex; i < endIndex; i++ {
				err := mc.Set(&memcache.Item{
					Key:   fmt.Sprintf("KEY%07d", i+1),
					Value: []byte(fmt.Sprintf("VALUE:%07d", i+1)),
				})
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Memcached SET 100,000, 4 threads:", time.Since(start))
}

func benchMemcachedGetBatch() {
	mc := memcache.New("localhost:11211")

	var wg sync.WaitGroup
	const numThreads = 4
	wg.Add(numThreads)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			total := 0
			for i := startIndex; i < endIndex; {
				const batchKeys = 40
				keys := make([]string, 0, batchKeys)
				for k := 0; k < batchKeys; k++ {
					key := fmt.Sprintf("KEY%07d", i+1)
					keys = append(keys, key)
					i++
				}
				total += len(keys)
				// values := fmt.Sprintf("VALUE:%07d", i+1)
				_, err := mc.GetMulti(keys)
				if err != nil {
					panic(err)
				}
			}
			fmt.Println("TOTAL:", total)
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Memcached GET 100,000, 4 threads:", time.Since(start))
}

func benchRedisSet() {
	client := redis.NewClient(&redis.Options{})

	var wg sync.WaitGroup
	const numThreads = 4
	wg.Add(numThreads)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			for i := startIndex; i < endIndex; i++ {
				key := fmt.Sprintf("KEY%07d", i+1)
				value := fmt.Sprintf("VALUE:%07d", i+1)

				err := client.Set(context.Background(), key, value, 0).Err()
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Redis SET 100,000, 4 threads:", time.Since(start))
}

func benchRedisGet() {
	client := redis.NewClient(&redis.Options{})

	var wg sync.WaitGroup
	const numThreads = 4
	wg.Add(numThreads)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			for i := startIndex; i < endIndex; i++ {
				key := fmt.Sprintf("KEY%07d", i+1)

				result := client.Get(context.Background(), key)
				val, err := result.Result()
				if err != nil {
					panic(err)
				}
				if len(val) != 13 {
					panic("Invalid value")
				}
			}
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Redis GET 100,000, 4 threads:", time.Since(start))
}

func benchRedisGetBatch() {
	client := redis.NewClient(&redis.Options{})

	var wg sync.WaitGroup
	const numThreads = 4
	wg.Add(numThreads)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			total := 0
			for i := startIndex; i < endIndex; {
				const batchKeys = 20
				keys := make([]string, 0, batchKeys)
				for k := 0; k < batchKeys; k++ {
					key := fmt.Sprintf("KEY%07d", i+1)
					keys = append(keys, key)
					i++
				}
				total += len(keys)
				// values := fmt.Sprintf("VALUE:%07d", i+1)
				_, err := client.MGet(context.Background(), keys...).Result()
				if err != nil {
					panic(err)
				}
			}
			fmt.Println("TOTAL:", total)
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Redis GET 100,000, 4 threads:", time.Since(start))
}

func main() {
	//benchMemcachedSet()
	//benchMemcachedGetBatch()

	benchRedisSet()
	benchRedisGetBatch()
}

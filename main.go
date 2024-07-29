package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	gocache "github.com/QuangTung97/go-memcache/memcache"
	cachestats "github.com/QuangTung97/go-memcache/memcache/stats"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redis/redis/v8"
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

func benchBradfitzMemcachedGetBatch() float64 {
	mc := memcache.New("localhost:11211")

	var wg sync.WaitGroup
	const numThreads = 8

	mc.MaxIdleConns = numThreads

	wg.Add(numThreads)

	const batchKeys = 160

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			total := 0
			for i := startIndex; i < endIndex; {
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

	duration := time.Since(start)
	fmt.Printf(
		"Duration for Bradfitz Memcached GET 100,000, %d threads, batch %d: %v\n",
		numThreads, batchKeys, duration,
	)

	return duration.Seconds() * 1000
}

func benchRedisSet() {
	client := redis.NewClient(&redis.Options{})

	var wg sync.WaitGroup
	const numThreads = 16
	wg.Add(numThreads)

	valuePrefix := strings.Repeat("ABCDE", valueSize/5)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100_000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			for i := startIndex; i < endIndex; i++ {
				key := fmt.Sprintf("KEY%07d", i+1)
				value := fmt.Sprintf("%s:%07d", valuePrefix, i+1)

				err := client.Set(context.Background(), key, value, 0).Err()
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Redis SET 100,000, 8 threads:", time.Since(start))
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

func benchRedisGetBatch() float64 {
	client := redis.NewClient(&redis.Options{
		MinIdleConns: 12,
	})

	var wg sync.WaitGroup
	const numThreads = 12
	wg.Add(numThreads)

	fmt.Println("NUM Threads:", numThreads)

	const batchKeys = 80

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100_000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()

			total := 0
			for i := startIndex; i < endIndex; {
				keys := make([]string, 0, batchKeys)
				for k := 0; k < batchKeys; k++ {
					key := computeKey(i)
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

	d := time.Since(start)
	fmt.Printf("Duration for Redis GET 100,000, %d threads, batch %d: %v\n",
		numThreads, batchKeys, d)
	return d.Seconds() * 1000
}

const valueSize = 1600

func benchMyMemcacheClientSet() {
	mc, err := gocache.New("localhost:11211", 1, gocache.WithBufferSize(128*1024))
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	const numThreads = 16
	wg.Add(numThreads)

	valuePrefix := strings.Repeat("ABCDE", valueSize/5)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			for i := startIndex; i < endIndex; i++ {
				p := mc.Pipeline()

				key := fmt.Sprintf("KEY%07d", i+1)

				value := []byte(fmt.Sprintf("%s:%07d", valuePrefix, i+1))
				p.MSet(key, value, gocache.MSetOptions{})

				p.Finish()
			}
		}()
	}
	wg.Wait()

	fmt.Printf("Duration for Memcached SET 100,000, %d threads: %v\n", numThreads, time.Since(start))
}

func computeKey(input int) string {
	input = input + 1

	var data [256]byte
	result := data[:0]

	var numberData [30]byte
	number := numberData[:0]

	if input == 0 {
		number = append(number, '0')
	}

	for input > 0 {
		number = append(number, '0'+byte(input%10))
		input = input / 10
	}

	result = append(result, "KEY"...)

	for i := 0; i < 7-len(number); i++ {
		result = append(result, '0')
	}

	for i := len(number) - 1; i >= 0; i-- {
		result = append(result, number[i])
	}

	return string(result)
}

func benchMyMemcacheClientGetBatch() float64 {
	const numConns = 12
	fmt.Println("My memcache num conns:", numConns)

	mc, err := gocache.New("localhost:11211", numConns,
		gocache.WithBufferSize(64*1024),
		gocache.WithTCPKeepAliveDuration(10*time.Second),
		gocache.WithWriteLimit(1600),
		gocache.WithMaxCommandsPerBatch(160),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := mc.Close()
		if err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	const numThreads = 12
	wg.Add(numThreads)

	const batchKeys = 80

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100_000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			total := 0

			fnList := make([]gocache.MGetResult, 0, batchKeys)

			for i := startIndex; i < endIndex; {
				keys := make([]string, 0, batchKeys)
				for k := 0; k < batchKeys; k++ {
					key := computeKey(i)
					keys = append(keys, key)
					i++
				}
				total += len(keys)

				p := mc.Pipeline()

				fnList = fnList[:0]

				for _, k := range keys {
					fn, err := p.MGetFast(k, gocache.MGetOptions{
						N:   3,
						CAS: true,
					})
					if err != nil {
						panic(err)
					}
					fnList = append(fnList, fn)
				}

				for _, fn := range fnList {
					resp, err := fn.Result()
					if err != nil {
						panic(err)
					}

					if len(resp.Data) != valueSize+8 {
						panic(len(resp.Data))
					}

					gocache.ReleaseGetResponseData(resp.Data)
					gocache.ReleaseMGetResult(fn)
				}

				p.Finish()
			}
			fmt.Println("TOTAL:", total)
		}()
	}
	wg.Wait()

	d := time.Since(start)
	fmt.Printf("Duration for My Memcached GET 100,000, %d threads, batch %d: %v\n", numThreads, batchKeys, d)
	return d.Seconds() * 1000
}

func benchMCGetBatchWithLatency() {
	const numConns = 8
	fmt.Println("My memcache num conns:", numConns)

	mc, err := gocache.New("localhost:11211", numConns,
		gocache.WithBufferSize(64*1024),
		gocache.WithTCPKeepAliveDuration(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := mc.Close()
		if err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	const numThreads = 8
	wg.Add(numThreads)

	const perThread = 100000

	const batchKeys = 40

	var mut sync.Mutex
	durations := make([]time.Duration, 0, numThreads*perThread)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			total := 0
			for i := startIndex; i < endIndex; {
				keys := make([]string, 0, batchKeys)
				for k := 0; k < batchKeys; k++ {
					key := fmt.Sprintf("KEY%07d", i+1)
					keys = append(keys, key)
					i++
				}
				total += len(keys)

				p := mc.Pipeline()

				getStart := time.Now()
				var fn func() (gocache.MGetResponse, error)
				for _, k := range keys {
					fn = p.MGet(k, gocache.MGetOptions{})
				}

				resp, err := fn()
				if err != nil {
					panic(err)
				}
				duration := time.Since(getStart)

				mut.Lock()
				durations = append(durations, duration)
				mut.Unlock()

				if len(resp.Data) != valueSize+8 {
					panic(len(resp.Data))
				}

				p.Finish()
			}
			fmt.Println("TOTAL:", total)
		}()
	}
	wg.Wait()

	fmt.Printf("Duration for My Memcached GET 100,000, %d threads, batch %d: %v\n",
		numThreads, batchKeys, time.Since(start))

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	n := len(durations)
	fmt.Println("MIN DURATION:", durations[0])
	fmt.Println("MEAN DURATION:", durations[n/2])
	fmt.Println("P99:", durations[int(float64(n-1)*0.99)])

	fmt.Println("MAX DURATION:", durations[n-1])
}

func runServer() {
	http.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	err := http.ListenAndServe(":10090", nil)
	if err != nil {
		panic(err)
	}
}

func main() {
	// go runServer()

	benchMyMemcacheClientSet()
	// benchMemcachedSet()
	// benchRedisSet()

	// benchRedisGetBatch()

	sum := float64(0)
	const numLoops = 30

	for i := 0; i < numLoops; i++ {
		sum += benchMyMemcacheClientGetBatch()
		// sum += benchBradfitzMemcachedGetBatch()
		// sum += benchRedisGetBatch()
	}

	avgAll := sum / float64(numLoops)
	fmt.Println("AVG ALL:", avgAll)
	fmt.Printf("AVG QPS: %.2f\n", 12*100_000.0*1000.0/avgAll)
}

func dumpKeys() {
	totalKeys := 0

	client := cachestats.New("localhost:11211")
	err := client.MetaDumpAll(func(key cachestats.MetaDumpKey) {
		totalKeys++
	})

	fmt.Println(err, totalKeys)
}

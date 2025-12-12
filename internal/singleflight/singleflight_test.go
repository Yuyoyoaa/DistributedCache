package singleflight

import (
	"sync"
	"testing"
	"time"
)

func TestSingleflightDo(t *testing.T) {
	var g Group

	count := 0
	fn := func() (interface{}, error) {
		count++
		time.Sleep(50 * time.Millisecond)
		return "ok", nil
	}

	const threads = 10
	var wg sync.WaitGroup
	wg.Add(threads)

	results := make([]interface{}, threads)

	for i := 0; i < threads; i++ {
		go func(i int) {
			defer wg.Done()
			v, _ := g.Do("same-key", fn)
			results[i] = v
		}(i)
	}

	wg.Wait()

	if count != 1 {
		t.Fatalf("expected fn to be called only once, but got %d", count)
	}

	for i := 0; i < threads; i++ {
		if results[i] != "ok" {
			t.Fatalf("unexpected result: %v", results[i])
		}
	}
}

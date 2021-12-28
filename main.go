package main

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

type Bucket struct {
	Cap   int
	Index int
	Start int
	sync.Mutex
}

func sequentialBucket(input []int, sliceLength, bucketWidth, nrBuckets int) {
	indexes := make(map[int]int, sliceLength)
	out := make([]int, sliceLength)
	buckets := make([]Bucket, nrBuckets)

	for i := 0; i < sliceLength; i++ {
		j := input[i] / bucketWidth
		if j > len(buckets)-1 {
			j = len(buckets) - 1
		}
		buckets[j].Cap++
		indexes[i] = j
	}

	for i := 1; i < len(buckets); i++ {
		buckets[i].Index = buckets[i-1].Index + buckets[i-1].Cap
		buckets[i].Start = buckets[i-1].Start + buckets[i-1].Cap
	}

	for i := 0; i < sliceLength; i++ {
		j := indexes[i]
		index := buckets[j].Index
		buckets[j].Index++
		out[index] = input[i]
	}

	for i := 0; i < len(buckets); i++ {
		s := out[buckets[i].Start:buckets[i].Index]
		sort.Ints(s)
	}

	sort.Ints(input)
	fmt.Printf("Is sorted? %v\n", reflect.DeepEqual(input, out))
}

func parallelBucket(input []int, sliceLength, bucketWidth, nrBuckets int) {
	var wg sync.WaitGroup
	indexes := make(map[int]int, sliceLength)
	out := make([]int, sliceLength)
	buckets := make([]Bucket, nrBuckets)

	wg.Add(sliceLength)
	for i := 0; i < sliceLength; i++ {
		go func(i int) {
			j := input[i] / bucketWidth
			if j > len(buckets)-1 {
				j = len(buckets) - 1
			}
			buckets[j].Mutex.Lock()
			defer buckets[j].Mutex.Unlock()
			buckets[j].Cap++
			indexes[i] = j
			wg.Done()
		}(i)
	}
	wg.Wait()

	buckets[0].Index += buckets[0].Cap
	for i := 1; i < nrBuckets; i++ {
		buckets[i].Index = buckets[i-1].Index + buckets[i].Cap
		buckets[i].Start = buckets[i-1].Start + buckets[i-1].Cap
	}

	wg.Add(sliceLength)
	for i := 0; i < sliceLength; i++ {
		go func(i int) {
			j := indexes[i]
			buckets[j].Mutex.Lock()
			defer buckets[j].Mutex.Unlock()
			index := buckets[j].Index - buckets[j].Cap
			buckets[j].Cap--
			out[index] = input[i]
			wg.Done()
		}(i)
	}

	wg.Wait()
	wg.Add(nrBuckets)

	for i := 0; i < nrBuckets; i++ {
		go func(i int) {
			s := out[buckets[i].Start:buckets[i].Index]
			sort.Ints(s)
			wg.Done()
		}(i)
	}
	wg.Wait()

	sort.Ints(input)
	fmt.Printf("Is sorted? %v\n", reflect.DeepEqual(input, out))
}

func main() {
	bucketWidth := 10
	sliceLength := 8
	nrBuckets := 5
	inputSeq := []int{29, 25, 3, 49, 9, 37, 21, 43}
	inputParallel := []int{29, 25, 3, 49, 9, 37, 21, 43}

	sequentialBucket(inputSeq, sliceLength, bucketWidth, nrBuckets)
	parallelBucket(inputParallel, sliceLength, bucketWidth, nrBuckets)
}

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var wg sync.WaitGroup

const BUCKET_SIZE = 5
const NUM_MESSAGES = 50

func main() {
	producer_channel := make(chan int, 10)
	consumer_channel := make(chan [5]int, 10)
	wg.Add(1)
	go processor(consumer_channel)
	wg.Add(1)
	go consumer(consumer_channel, producer_channel)
	wg.Add(1)
	go producer(producer_channel)

	wg.Wait()

}

func processor(consumer_channel chan [5]int) {
	ind := 0
	for val := range consumer_channel {
		fmt.Print("Bucket ", ind, ":")
		for _, i := range val {
			fmt.Print(" ", i)
		}
		fmt.Println()
		ind++
	}
	wg.Done()
}

type Bucket struct {
	Items    [5]int
	Inserted int
}

func consumer(consumer_channel chan [5]int, producer_channel chan int) {
	buckets := make(map[int]*Bucket)
	var bucket_to_send int = 0
	for message := range producer_channel {
		var bucket_num int = message / BUCKET_SIZE
		//fmt.Println(bucket_num)
		//fmt.Println("map:", buckets)
		if val, ok := buckets[bucket_num]; ok {
			val.Items[message%BUCKET_SIZE] = message
			val.Inserted++

			// if the bucket is full, start passing them on
			for ok && val.Inserted == BUCKET_SIZE && bucket_num == bucket_to_send {
				consumer_channel <- val.Items
				val.Inserted = 0

				bucket_to_send++
				bucket_num++

				val, ok = buckets[bucket_num]
			}
		} else {
			buckets[bucket_num] = &Bucket{}
			some_val := buckets[bucket_num]
			some_val.Items[message%BUCKET_SIZE] = message
			some_val.Inserted++
		}
	}
	ok := true
	var val *Bucket
	for ok {
		if val, ok = buckets[bucket_to_send]; ok {
			consumer_channel <- val.Items
			bucket_to_send++
		}
	}

	close(consumer_channel)
	wg.Done()
}

func producer(producer_channel chan int) {
	rand.Seed(time.Now().UnixNano())
	nums := rand.Perm(NUM_MESSAGES)

	rand.Shuffle(len(nums), func(i, j int) {
		nums[i], nums[j] = nums[j], nums[i]
	})

	for _, val := range nums {
		producer_channel <- val
	}

	close(producer_channel)
	wg.Done()
}

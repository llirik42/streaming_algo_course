package main

import (
	"fmt"
	"kvschool/internal/sstable"
	"log"
	"math/rand"
	"os"
)

func stringToBytes(s string) []byte {
	return []byte(s)
}

func generateRandomStringByLength(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func generateRandomString() string {
	if rand.Int31()%4 == 0 {
		return generateRandomStringByLength(4096)
	}

	return generateRandomStringByLength(32)
}

func write() {
	file, err := os.Create("test.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := sstable.NewWriter(file)
	defer writer.Close()

	for i := 0; i < 10; i++ {
		//value := generateRandomString()

		value := fmt.Sprintf("value-%d", i+1)

		if err := writer.Add(stringToBytes(fmt.Sprintf("key-%d", i+1)), stringToBytes(value)); err != nil {
			log.Fatal(err)
		}
	}
}

func read() {
	file, err := os.Open("test.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	r, err := sstable.NewReader(file, info.Size())
	if err != nil {
		log.Fatal(err)
	}

	it, err := r.Iterator(nil, nil)
	defer it.Close()
	if err != nil {
		log.Fatal(err)
	}

	for {
		key, value, ok, err := it.Next()
		if err != nil {
			log.Fatal(err)
			break
		}
		if !ok {
			break
		}

		fmt.Printf("%s:%s\n", key, value)
	}
}

func main() {
	write()
	read()

}

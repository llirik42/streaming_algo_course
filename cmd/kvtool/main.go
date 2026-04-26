package main

import (
	"fmt"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
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

func walDemo() {
	file, err := os.Create("/home/llirik42/wal")
	if err != nil {
		log.Fatal(err)
	}

	writer := wal.NewWriter(file)
	rec1 := wal.Record{
		Type:  wal.OpPut,
		Key:   stringToBytes("key1"),
		Value: stringToBytes("value1"),
	}
	rec2 := wal.Record{
		Type: wal.OpDelete,
		Key:  stringToBytes("key2"),
	}
	if err := writer.Append(rec1); err != nil {
		log.Fatal(err)
	}
	if err := writer.Append(rec2); err != nil {
		log.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	reader := wal.NewReader(file, fileStat.Size())
	if err := reader.ValidateChecksums(); err != nil {
		log.Fatal(err)
	}
}

func sstableDemo() {
	file, err := os.Create("/home/llirik42/sstable")
	if err != nil {
		log.Fatal(err)
	}

	writer := sstable.NewWriter(file)
	if err := writer.Add(stringToBytes("key1"), stringToBytes("value1")); err != nil {
		log.Fatal(err)
	}
	if err := writer.Add(stringToBytes("key2"), stringToBytes("value2")); err != nil {
		log.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	reader, err := sstable.NewReader(file, fileStat.Size())
	if err != nil {
		log.Fatal(err)
	}
	if err := reader.ValidateChecksum(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s-%s\n", reader.GetFirstKey(), reader.GetLastKey())
}

func main() {
	//defer func() {
	//	fmt.Printf("Exiting...\n")
	//}()
	//
	//time.Sleep(3 * time.Second)

	//walDemo()
}

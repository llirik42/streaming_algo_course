package main

import (
	"fmt"
	"kvschool/internal/lsm"
	"math/rand"
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

func main() {
	options := lsm.Options{
		Dir:                    "/home/llirik42/db",
		MemtableFlushThreshold: 1048576,
	}

	engine, err := lsm.Open(options)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", engine)

	if err := engine.Put(stringToBytes("key"), stringToBytes("value-3")); err != nil {
		panic(err)
	}
	value, err := engine.Get(stringToBytes("key"))
	if err != nil {
		fmt.Printf("%v", err)
	} else {
		fmt.Printf("%+s\n", value)
	}

	engine.Close()

	//defer func(engine *lsm.Engine) {
	//	err := engine.Close()
	//	if err != nil {
	//		panic(err)
	//	}
	//}(engine)
	//
	//if err := engine.Put(stringToBytes("key"), stringToBytes("value")); err != nil {
	//	panic(err)
	//}
}

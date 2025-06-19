package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// defines the operations applicable to all cache types (in this case, LRU and LFU)
type Cache interface {
	Get(key uint64) string
	Delete(key uint64, ttlHeap *TTLHeap)
	Put(key uint64, value string, clientKey string, ttlHeap *TTLHeap, keyMap map[string]uint64) string
}

type AppCache struct {
	//Name of the tenant, i.e, App Name
	Name string

	// map client key (string) to the key used in the cache (uint64)
	KeyMap map[string]uint64

	// lru cache
	// key - value retrieved from keyMap for a given client key
	// value - user input value for the given client key
	Lru *LRUCache

	// lfu cache
	// key - value retrieved from keyMap for a given client key
	// value - user input value for the given client key
	Lfu *LFUCache

	//mutex lock (for set/get operations)
	Mu sync.Mutex

	//TTL heap to keep track of the keys and their timestamps
	TTLHeap TTLHeap

	//Max TTL
	MaxTTL time.Duration
}

// global mutex lock to ensure that keyMap and freedKeys are accessed/modified in a thread-safe manner
var gmu sync.Mutex

// counter to keep track of the next key to be used in the cache
var keyCounter uint64 = 1

// stores list of freed cache keys (key counter) when a key is deleted so that they can be reused later
var freedKeys []uint64

// ticker to keep periodically checking the cache for expired keys
var ticker *time.Ticker

// map appId (tenantId) to AppCache
var tenantCache map[uint64]*AppCache

// retrieve the size of data (in bytes) to be inserted into the cache
func entrySize(clientKey string, value string) uint64 {
	//calculate the size of the entry in bytes
	//size of the entry = size of clientKey + size of value
	return uint64(len(clientKey)) + uint64(len(value))
}

// retrieve the appropriate cache for a given tenant
func getCache(tenantId uint64) (Cache, error) {
	if _, ok := tenantCache[tenantId]; !ok {
		return nil, errors.New("tenant does not exist")
	}
	if tenantCache[tenantId].Lru != nil {
		return tenantCache[tenantId].Lru, nil
	}
	return tenantCache[tenantId].Lfu, nil
}

// periodically check and evict expired keys from the cache for all tenants
func ttlCheck() {
	for range ticker.C {
		for tenant := range tenantCache {
			//lock the next section for a given tenant
			tenantCache[tenant].Mu.Lock()
			//retrieve the min element from the ttl heap
			minTTL, heapLen := getMinTTL(&tenantCache[tenant].TTLHeap)

			//retrieve the cache (LRU/LFU per configuration) for the given tenant
			//this is easier than explicitly checking in each operation if the cache type is LRU or LFU
			tcache, err := getCache(tenant)
			if err != nil {
				log.Printf("Error retrieving cache for tenant when performing ttl check %d: %v\n", tenant, err)
				continue
			}

			//pop all elements from the heap that have expired
			//by checking if the difference between the current time and the timestamp of the min element is greater than ttl seconds
			//make sure the second condition is configurable in future (currently set to 10 seconds)
			for heapLen > 0 && time.Since(minTTL.Timestamp) > tenantCache[tenant].MaxTTL*time.Second {
				//delete the key from the cache (the cache Delete operation also takes care of removing the min element from the heap)
				tcache.Delete(minTTL.Key, &tenantCache[tenant].TTLHeap)

				//delete the key from the keyMap
				delete(tenantCache[tenant].KeyMap, minTTL.ClientKey)

				//add the key to the freedKeys list (lock this section globally)
				gmu.Lock()
				freedKeys = append(freedKeys, minTTL.Key)
				gmu.Unlock()

				//retrieve the next min element from the heap
				minTTL, heapLen = getMinTTL(&tenantCache[tenant].TTLHeap)
			}
			tenantCache[tenant].Mu.Unlock()
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	//create a buffer to hold the incoming data
	buffer := make([]byte, 1024)

	//read streamed data from the connection and keeps connection open for the client
	//until the client closes the connection
	for {
		n, err := conn.Read(buffer)
		fmt.Println(n)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client closed connection")
			} else {
				fmt.Printf("Error reading data: %v\n", err)
			}
			return
		}
		//process the received data
		data := string(buffer[:n])

		//trim leading and trailing whitespace and newlines from the data
		data = strings.TrimSpace(data)

		fmt.Printf("Received data: %s\n", data)

		//parse the data contents (of the form CMD <key> <value (optional if using set)>)
		//eg.
		//1. GET 1 k1 (tenantId -> 1, cmd -> GET, clientKey -> k1)
		//2. SET 1 k1 hello (tenantId -> 1, cmd->SET, clientKey -> k1, clientValue -> hello)
		tenantIdStr, clientData, _ := strings.Cut(data, " ")
		cmd, kv, _ := strings.Cut(clientData, " ")
		clientKey, clientValue, _ := strings.Cut(kv, " ")

		tenantId, _ := strconv.ParseUint(tenantIdStr, 10, 64)

		//if the tenant does not exist, return an error to the client and continue to the next iteration
		if _, ok := tenantCache[tenantId]; !ok {
			log.Printf("Tenant %d does not exist\n", tenantId)
			conn.Write([]byte(fmt.Sprintf("Error: Tenant %d does not exist\n", tenantId)))
			continue
		}

		//retrieve the cache (LRU/LFU per configuration) for the given tenant
		//this is easier than explicitly checking in each operation if the cache type is LRU or LFU
		tcache, err := getCache(tenantId)
		if err != nil {
			log.Printf("Error retrieving cache for tenant %d: %v\n", tenantId, err)
			conn.Write([]byte(fmt.Sprintf("Error: %v\n", err)))
			continue
		}

		//retrieve the args for the command if provided
		var keyCache uint64
		var cacheHit bool
		var usingFreedKey bool = false
		if len(cmd) > 0 {
			//retrieve the cache key (uint64) from the client key (string) if it exists
			//else 'create' a new cache key for the user provided key
			if _, cacheHit = tenantCache[tenantId].KeyMap[clientKey]; cacheHit {
				keyCache = tenantCache[tenantId].KeyMap[clientKey]
			} else {
				//if a cache key has been freed/deleted previously, use this key, set keyCache and remove it from freedKeys
				//else set keyCache to the value of keyCounter and increment keyCounter
				//lock the global mutex to ensure that freedKeys and keyCounter are accessed/modified in a thread-safe manner
				gmu.Lock()
				if len(freedKeys) > 0 {
					tenantCache[tenantId].KeyMap[clientKey] = freedKeys[0]
					keyCache = freedKeys[0]
					freedKeys = freedKeys[1:]
					usingFreedKey = true

				} else {
					tenantCache[tenantId].KeyMap[clientKey] = keyCounter
					keyCache = keyCounter
					keyCounter++
				}
				gmu.Unlock()
			}
		}

		//if the command is
		//1. GET <key> return the value for the key
		//2. SET <key> <value> set/update the value for the key
		//3. DEL <key> delete the key
		//4. any other command, return an error
		switch cmd {
		case "GET":
			//abort the operation (skip current iteration) if get does not have exactly one argument
			if len(clientKey) == 0 {
				log.Printf("No key provided\n")
				conn.Write([]byte("Error: No key provided\n"))
				continue
			}

			//if there is a cache hit:
			//1. return the value from the cache corresponding to the cache key
			//if there is a cache miss
			//1. decrement keyCounter if a (previously) freed key is not being used
			//2. if a freed key was used, push it back into the freedKeys list
			//3. delete the client key from keyMap
			if cacheHit {
				//lock the tenant cache for the given tenantId to ensure thread-safety
				tenantCache[tenantId].Mu.Lock()
				v := tcache.Get(keyCache)
				tenantCache[tenantId].Mu.Unlock()

				conn.Write([]byte(v))
				conn.Write([]byte("\n"))
			} else {
				gmu.Lock()
				if !usingFreedKey {
					keyCounter--
				} else {
					freedKeys = append(freedKeys, keyCache)
				}
				gmu.Unlock()

				tenantCache[tenantId].Mu.Lock()
				delete(tenantCache[tenantId].KeyMap, clientKey)
				tenantCache[tenantId].Mu.Unlock()

				conn.Write([]byte("Key not found\n"))
			}

		case "SET":
			//abort the operation (skip current iteration) if set does not have exactly two arguments
			if len(clientValue) == 0 {
				log.Printf("Incorrect argument count for SET command\n")
				conn.Write([]byte("Error: Incorrect argument count for SET command\n"))
				continue
			}
			//set/update the value for the key
			//return OK to the client
			tenantCache[tenantId].Mu.Lock()
			res := tcache.Put(keyCache, clientValue, clientKey, &tenantCache[tenantId].TTLHeap, tenantCache[tenantId].KeyMap)
			tenantCache[tenantId].Mu.Unlock()

			conn.Write([]byte(fmt.Sprintf("%s\n", res)))

		case "DEL":
			//abort the operation (skip current iteration) if del does not have exactly two arguments
			if len(clientKey) == 0 {
				log.Printf("No key provided\n")
				conn.Write([]byte("Error: No key provided\n"))
				continue
			}

			//if there is a cache hit:
			//1. delete the entry in the cache
			//2. delete the client key (and it's value) from the keyMap
			//3. append the freed key (keyCache) to the freedKeys list
			//if there is a cache miss
			//1. decrement keyCounter if a (previously) freed key is not being used
			//2. if a freed key was used, push it back into the freedKeys list
			//3. delete the client key from keyMap
			if cacheHit {
				tenantCache[tenantId].Mu.Lock()
				tcache.Delete(keyCache, &tenantCache[tenantId].TTLHeap)
				delete(tenantCache[tenantId].KeyMap, clientKey)
				tenantCache[tenantId].Mu.Unlock()

				gmu.Lock()
				freedKeys = append(freedKeys, keyCache)
				gmu.Unlock()

				conn.Write([]byte(fmt.Sprintf("Deleted key %s\n", clientKey)))
			} else {
				gmu.Lock()
				if !usingFreedKey {
					keyCounter--
				} else {
					freedKeys = append(freedKeys, keyCache)
				}
				gmu.Unlock()

				tenantCache[tenantId].Mu.Lock()
				delete(tenantCache[tenantId].KeyMap, clientKey)
				tenantCache[tenantId].Mu.Unlock()

				conn.Write([]byte(fmt.Sprintf("Key %s does not exist in cache\n", clientKey)))
			}

		default:
			log.Printf("Unknown command: %s\n", cmd)
			conn.Write([]byte("Error: Unknown command\n"))
			continue
		}
	}
}

func main() {
	//create tcp server
	port := ":9001"
	listener, err := net.Listen("tcp", port)

	//if there is an error retrieving the listener, log it and exit
	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()

	fmt.Printf("Server is listening on port %s\n", port)

	//add config logic here in the future (retrieve information such as maxmemory, maxttl, eviction policy, etc.)

	//initialize the tenant cache
	tenantCache = make(map[uint64]*AppCache)

	//read config file and subsequently load the config into the tenantCache map
	configs, err := readConfig("config.json")
	if err != nil {
		log.Fatal("Error reading config file:", err)
	}
	//load the config into the tenantCache map
	loadConfig(tenantCache, configs)

	//periodically check the cache for expired keys
	//make the ticker time configurable in the future
	ticker = time.NewTicker(3 * time.Second)
	go ttlCheck()

	//accept connections and handle them in their own goroutines
	for {
		conn, err := listener.Accept()
		//if there is an error accepting the connection, log it and continue to listen for new connections
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		//handle the connection in a goroutine (each client gets it's own goroutine)
		go handleConnection(conn)
	}
}

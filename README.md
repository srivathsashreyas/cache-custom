# Overview
This is an implementation of a multi tenant cache in Go. The cache is implemented as a TCP server to minimize latency in retrieving data. Although similar functionality can be achieved using well known alternatives such as Redis/Valkey; this implementation is meant to be a lightweight, cache focussed solution. Additionally, eviction policies are configurable per tenant on a single instance of the cache. As of time of writing, this is not available in Redis/Valkey (which support namespaces but not multiple eviction policies). 

# How to Run 
1. `go build -o go_cache cmd/cache-custom/*.go`
2. `./go_cache` (ensure that the config.json file is available in the same directory as the executable)

# Customization
1. Tenant specific config can be added to config.json. It accepts an array of tenant objects. Following is the structure  expected in the config.json file:

```
[
	{
		"Name": <String; app/tenant name>,
		"AppId": <Uint64; app/tenant id>,
		"MaxMemory": <Uint64; max memory in bytes>,
		"Lru": <bool; enable LRU eviction policy>,
		"Lfu": <bool; enable LFU eviction policy>,
		"MaxTTL": <max time to live in seconds>,
	},
    ...
]
```
Note that only one eviction policy can be enabled per tenant

2. To avoid having to deal with ttl based evictions; set an arbitrary high value (for example, 31536000 which is equivalent to a year) for MaxTTL. This will ensure that the cache does not evict entries based on TTL expiry

# How It Works
1. TCP Server
    - Tenant configurations are loaded from config.json 
    - The server listens on port 9001
    - Each tcp client connection is handled in a separate goroutine which is kept alive until the client disconnects
    - For a given tenant, the relevant cache operations are handled under a mutex lock to ensure thread safety
2. Cache Operations
    - The following operations are supported for both cache types (LRU and LFU):
        - <tenantId> GET <key> - Return the value for the given key if it exists; not found otherwise
        - <tenantId> SET <key> <value> - Set the value for the given key. If the key already exists, it is updated (if memory constraints are respected). If the key does not exist and space is insufficient, the eviction policy is applied to make space for the new key value pair 
        - <tenantId> DEL <key> - Delete the key if it exists; return key doesn't exist otherwise
4. TTL
    - Every entry in the cache has a TTL (time to live) which is based on the configuration for the tenant. When a cache entry is SET (could either be a new entry or an update of an existing entry), the current time (time.Now()) is stored   
    - A periodic check is performed (every 3s) to identify if there are entries that need to be evicted based on their TTL
    - A min heap of TTLs (per tenant) is maintained to ensure that the cache can efficiently identify expiring entries and evict them when necessary. The TTL heap is maintained based on the 'SET' timestamp for the entry. with entries that were 'SET' earlier being at the top of the heap (and therefore, more likely candidates for eviction)
    - The periodic check compares the current time with the timestamp of the entries at the top of the heap and evicts them if they have expired (exceeded the TTL for that specific tenant). This continues until an entry is found that has not expired or the heap is empty  
    - TTL eviction takes place independently of the eviction policy. It takes precedence over the eviction policy since it is based on the time to live configured for the tenant. However, for entries that have not expired, the eviction policy is applied when attempting to add a new entry that exceeds the memory limit for the tenant 
5. TCP Client
    - To test the cache, the easiest solution is to use a TCP client such as ncat
    - Simply run `ncat localhost 9001` (assuming the TCP server is running on localhost:9001) 
    - This establishes a TCP connection to the cache server. Following this commands can be entered in the format `<tenantId> <command> <key> <value>` (where value is required only for the SET command). For example:
        - `1 SET key1 value1` - This sets the value for key1 in tenant 1 (respecting the cache config for tenant 1)
        - `1 GET key1` - This retrieves the value for key1 in tenant 1 (if it exists)
        - `1 DEL key1` - This deletes key1 in tenant 1 (if it exists)
    - Alternatively a TCP client can be implemented in a language of the users' choice with commands sent in the aforementioned format

# Potential Improvements
1. Periodic Data Dump: Allows for a backup of cached data in case the TCP server crashes or is restarted
2. Additional Eviction Policies: Introduce random and FIFO eviction policies
3. Additional Commands: Examples, FLUSH -> flush entries per tenant or for all tenants. STATS -> get stats for a tenant (number of entries, memory used, eviction policy, etc.) 
// CStorage package is module for provide key - value cache storage
// Outsiders can use following; Get, Put, Delete, Clear, which are self explanatory
package cstorage

import (
	"sync"
	"time"
)

func main() {
	println("hello this is cache-storage package")
}

// CStorage structure is struct for holding data structure and misc of cache storage.
// CStorage uses hash table, and doubly linked list for eviction policy.
// - Hash Table: since CStorage is key-value store, hash table should be good choice since it has O(logN) to insert and search.
// - Double Linked List: length of data would be limited, and eviction will be happen in LRU manner(Least Recently Used). To implement this, I will use double linked list here.
type CStorage struct {
	table  map[string]*node
	head   *node
	tail   *node
	size   int64
	mutex  *sync.Mutex
	config CStorageConfig
}

// CStorageConfig structure should be provided when outside code calls New() function. It will set properties of storage such as ttl or capacity
type CStorageConfig struct {
	Ttl      time.Duration
	Capacity int64
}

// New function is initializer of CStorage. It takes CStorageConfig as parameter, which acts as configuration, and returns the pointer to CStorage.
func New(config CStorageConfig) *CStorage {
	return &CStorage{
		table:  make(map[string]*node),
		head:   nil,
		tail:   nil,
		size:   0,
		mutex:  &sync.Mutex{},
		config: config,
	}
}

// node is internal structure of cache storage. It has key which is key in hashmap, data, ttl which is time to live, prev which is pointer to previous node in linked list, next which is vise versa.
type node struct {
	key  string
	data []byte
	ttl  time.Time
	prev *node
	next *node
}

// Get function is to get data with key in cache storage. Since CStorage is key-value store, data can be found by key.
// If Get function is called, following would be happen
// - Search hashmap
// - If there is no data with key, it will return empty data with hit=false
// - If ttl is expired, it will delete record and return hit=false
// - If none of above, it will move the node by eviction policy, and return data with hit=true
func (s *CStorage) Get(key string) (data []byte, hit bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	n, ok := s.table[key]
	if !ok {
		return nil, false
	}

	if n.ttl.Before(time.Now()) {
		s.evict(n)
		s.size--
		return nil, false
	}

	return n.data, true
}

// Put function is to upsert data with key in cache storage. It will return hit=true if it is update or hit=false if the key didn't existed before.
// If Put function is called following will happen
// - Search hashmap with provided key
// - If key is there, just update data, renew ttl, move node according to eviction policy, return hit=true
// - If key is not there, next will happen
// - Check storage size is full, if full, remove one node in accordance to eviction policy
// - Push key-data to hashmap, place it with eviction policy, return hit=false
// *Note that hit is just key hits. Not the operation is successful or not.
func (s *CStorage) Put(key string, data []byte) (hit bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	n, ok := s.table[key]

	ttl := time.Now().Add(s.config.Ttl)

	if ok {
		s.table[key].data = data
		s.table[key].ttl = ttl
		s.setHead(n)
		return true
	}

	for s.size >= s.config.Capacity {
		s.evict(s.tail)
		s.size--
	}

	newNode := &node{
		key:  key,
		data: data,
		ttl:  ttl,
	}
	s.table[key] = newNode
	s.setHead(newNode)
	s.size++

	return false
}

// Delete function is to manually deletes key-value from CStorage.
func (s *CStorage) Delete(key string) (hit bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	node, ok := s.table[key]
	if !ok {
		return false
	}

	s.evict(node)
	s.size--

	return true
}

// Clear function is to clear all data from CStroage.
func (s *CStorage) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for s.head != nil {
		s.evict(s.tail)
	}
	s.size = 0
}

// Size function will return current size of CStorage
// *Note that in this version, CStorage will hold expired key since ttl deletion will passively happens
func (s *CStorage) Size() (size int64) {
	return s.size
}

// Cap function will return maximum size(capacity) of CStorage.
func (s *CStorage) Cap() (capacity int64) {
	return s.config.Capacity
}

// RemoveExpired function will traverse CStorage and will remove all expired key.
// Since current version of CStorage uses passive method for ttl, it is possible for CStorage to hold already expired key.
// This function should be called in regular basis to avoid memory efficiency
func (s *CStorage) RemoveExpired() int64 {
	var count int64 = 0
	for key, value := range s.table {
		if value.ttl.Before(time.Now()) {
			s.Delete(key)
			count++
		}
	}
	return count
}

// setHead function is move node to head of linked list
func (s *CStorage) setHead(n *node) {
	if n == nil {
		return
	}

	if s.head == nil {
		s.head = n
		s.tail = n
		return
	}

	if s.head == n {
		return
	}

	if s.tail == n {
		s.tail = s.tail.prev
		s.tail.next = nil
		n.prev = nil
		s.head.prev = n
		n.next = s.head
		s.head = n
		return
	}

	if n.prev != nil {
		n.prev.next = n.next
	}

	if n.next != nil {
		n.next.prev = n.prev
	}

	n.prev = nil
	s.head.prev = n
	n.next = s.head
	s.head = n
}

// evict is to evict node from linked list and hash map
func (s *CStorage) evict(n *node) {
	if s.head == s.tail && s.head == n {
		s.head = nil
		s.tail = nil
		n.prev = nil
		n.next = nil
		delete(s.table, n.key)
		return
	}

	if s.head == n {
		s.head = s.head.next
		s.head.prev = nil
		n.prev = nil
		n.next = nil
		delete(s.table, n.key)
		return
	}

	if s.tail == n {
		s.tail = s.tail.prev
		s.tail.next = nil
		n.prev = nil
		n.next = nil
		delete(s.table, n.key)
		return
	}

	if n.prev != nil {
		n.prev.next = n.next
	}

	if n.next != nil {
		n.next.prev = n.prev
	}

	n.prev = nil
	n.next = nil
	delete(s.table, n.key)
}

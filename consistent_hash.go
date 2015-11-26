package main

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"sync"
)


type CircleHash struct {
	Nodes Nodes
	sync.Mutex
}


type Node struct {
	ID     string
	HashID uint32
}


type Nodes []*Node


func NewCircleHash() *CircleHash {
	return &CircleHash{Nodes: Nodes{}}
}

func hashID(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *CircleHash) AddNode(id string) {
	c.Lock()
	defer c.Unlock()

	node := NewNode(id)
	c.Nodes = append(c.Nodes, node)

	sort.Sort(c.Nodes)
}


func (c *CircleHash) searchost(id string) int {
	searchHost := func(it int) bool {
		return c.Nodes[it].HashID >= hashID(id)
	}
	return sort.Search(c.Nodes.Len(), searchHost)
}


func (c *CircleHash) Get(id string) string {
	i := c.searchost(id)
	if i >= c.Nodes.Len() {
		i = 0
	}
	return c.Nodes[i].ID
}


func NewNode(id string) *Node {
	return &Node{
		ID:     id,
		HashID: hashID(id),
	}
}

func (n Nodes) Less(i, j int) bool {
	return n[i].HashID < n[j].HashID
}

func (n Nodes) Len() int {
	return len(n)
}

func (n Nodes) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func main() {

	r := NewCircleHash()

	// array of servers
	host := []string{"http://localhost:3000", "http://localhost:3001", "http://localhost:3002"}

	//array of input objects as key and value
	arrayinput := []string{"1,a", "2,b", "3,c", "4,d", "5,e", "6,f", "7,g", "8,g", "9,h", "10,i"}

	// adding host in the ring
	for i := 0; i < len(host); i++ {
		r.AddNode(host[i])
	}

	// sending objects across servers.
	for i := 0; i < len(arrayinput); i++ {
		splitkeyval := strings.Split(arrayinput[i], ",")
		insertnode := r.Get(splitkeyval[0])
		putrestcall(insertnode, splitkeyval[0], splitkeyval[1])
		fmt.Println()
		getid(insertnode, splitkeyval[0])
	}

}

// Consume server PUT call
func response(host string, ikey string, ivalue string) {
	urlreq := host + "/keys/" + ikey + "/" + ivalue
	fmt.Printf("\n PUT URL: %s", urlreq)
	fmt.Printf("\n Key: %s and value: %s is inserted in server %s", ikey, ivalue, host)
	req, _ := http.NewRequest("PUT", urlreq, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}

//Consume server GET by id
func getid(host string, ikey string) {
	urlreq := host + "/keys/" + ikey
	fmt.Printf("\n GET URL: %s", urlreq)
	fmt.Printf("\n Fetching key: %s from server: %s", ikey, host)
	req, _ := http.NewRequest("GET", urlreq, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("\n Response:", string(body))
	defer resp.Body.Close()
}
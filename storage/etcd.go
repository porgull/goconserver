package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/xcat2/goconserver/common"
	"github.com/xcat2/goconserver/storage/etcd"
	"stathat.com/c/consistent"
)

const (
	STORAGE_ETCD    = "etcd"
	LOCK_PREFIX     = "lock"
	NODE_PREFIX     = "node"
	ENDPOINT_PREFIX = "endpoint"
)

func init() {
	STORAGE_INIT_MAP[STORAGE_ETCD] = newEtcdStorage
}

// EtcdKeyJoin is a convenience function for generating paths in etcd
func EtcdKeyJoin(elems ...string) string {
	paths := make([]string, 1, len(elems)+1)
	paths[0] = "/" + serverConfig.Etcd.Prefix
	paths = append(paths, elems...)
	for i, e := range paths {
		if e != "" {
			return strings.Join(paths[i:], "/")
		}
	}
	return ""
}

// EtcdStorage allows you to cluster goconserver
type EtcdStorage struct {
	*Storage
	client *etcd.EtcdClient
	host   string
	// vhost is for the business service, host is the real hostname
	vhost string

	leader     bool
	leaderLock *sync.RWMutex
	nodesFile  NodesFile
	syncToFile bool
}

func newEtcdStorage() StorInterface {
	var err error
	var hostname string
	stor := new(Storage)
	stor.async = true
	stor.Nodes = make(map[string]*Node)
	etcdStor := new(EtcdStorage)
	etcdStor.leaderLock = &sync.RWMutex{}
	etcdStor.Storage = stor
	etcdStor.nodesFile = NewNodesFile()
	hostname, err = os.Hostname()
	if err != nil {
		panic(err)
	}
	etcdStor.vhost = serverConfig.Etcd.Vhost
	if etcdStor.vhost == "" {
		etcdStor.vhost = hostname
	}
	etcdStor.host = serverConfig.Global.Host
	if etcdStor.host == "" || etcdStor.host == "0.0.0.0" {
		etcdStor.host = hostname
	}
	etcdStor.syncToFile = serverConfig.Etcd.SyncToFile
	// As client is used to send keepalive request, the client would not be closed
	etcdStor.client, err = etcd.NewEtcdClient(&serverConfig.Etcd)
	if err != nil {
		panic(err)
	}
	ready := make(chan struct{})
	go etcdStor.keepalive(ready)
	if etcdStor.syncToFile {
		// if syncing to file is enabled,
		// let's start an election to see
		// which instance will keep the
		// file in sync
		go etcdStor.runElection()
	}
	<-ready
	return etcdStor
}

func (e *EtcdStorage) keepalive(ready chan<- struct{}) {
	config := NewEndpointConfig(serverConfig.API.Port, serverConfig.Etcd.RpcPort, serverConfig.Console.Port, e.host)
	b, err := config.ToByte()
	if err != nil {
		panic(err)
	}
	s := string(b)
	for {
		err := e.client.RegisterAndKeepalive(EtcdKeyJoin(LOCK_PREFIX, e.vhost), EtcdKeyJoin(ENDPOINT_PREFIX, e.vhost), s, ready)
		if err != nil {
			plog.Error("Failed to register service")
		}
		time.Sleep(time.Duration(serverConfig.Etcd.ServiceHeartbeat) * time.Second)
	}
}

func (e *EtcdStorage) runElection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	election := etcd.Election{
		Client:           e.client.Cli,
		ElectionName:     "goconserver",
		CandidateName:    e.vhost,
		TTL:              10,
		ResumeLeader:     true,
		ReconnectBackOff: time.Second * 2,
	}
	leadershipChan, err := election.Run(ctx)
	if err != nil {
		plog.Error(fmt.Sprintf("Unable to do leadership election, won't sync to file...: %s", err.Error()))
		e.leader = false
		return
	}

	for {
		select {
		case isLeader, ok := <-leadershipChan:
			if !ok {
				plog.Error("Leadership channel closed, assuming no longer leader and exiting goroutine...")

				e.leaderLock.Lock()
				e.leader = false
				e.leaderLock.Unlock()
				return
			}

			e.leaderLock.Lock()
			e.leader = isLeader
			if isLeader {
				plog.Info("This goconserver was elected leader, will now handle etcd to nodes.json sync")
			}
			e.leaderLock.Unlock()

			// should sync happen here?
			if isLeader {
				e.Sync()
			}

		}
	}
}

// GetEndpoint gets a single endpoint for the given vhost
func (e *EtcdStorage) GetEndpoint(vhost string) (*EndpointConfig, error) {
	b, err := e.client.Get(EtcdKeyJoin(ENDPOINT_PREFIX, vhost))
	if err != nil {
		return nil, err
	}
	config := new(EndpointConfig)
	err = json.Unmarshal(b, config)
	if err != nil {
		plog.Error(err)
		return nil, err
	}
	return config, nil
}

// GetVhosts gets the endpoints in etcd
func (e *EtcdStorage) GetVhosts() (map[string]*EndpointConfig, error) {
	m, err := e.client.List(EtcdKeyJoin(ENDPOINT_PREFIX))
	if err != nil {
		return nil, err
	}
	ret := make(map[string]*EndpointConfig)
	for k, v := range m {
		config := new(EndpointConfig)
		err = json.Unmarshal([]byte(v), config)
		if err != nil {
			plog.Error(err)
			return nil, err
		}
		temp := strings.Split(k, "/")
		vhost := temp[len(temp)-1]
		ret[vhost] = config
	}
	return ret, nil
}

// ImportNodes imports nodes into storage
func (e *EtcdStorage) ImportNodes() {
	e.ImportInto(e.Storage.Nodes) // ? should e.Storage.Nodes be reset every import?
}

// ImportInto imports the nodes from the cluster into the given map
func (e *EtcdStorage) ImportInto(nodes map[string]*Node) {
	kvs, err := e.client.List(EtcdKeyJoin(NODE_PREFIX))
	if err != nil {
		plog.Error(fmt.Sprintf("Unable to import node from storage, Error: %s", err))
		panic(err)
	}
	for _, v := range kvs {
		node, err := UnmarshalNode([]byte(v))
		if err != nil {
			continue
		}
		nodes[node.Name] = node
	}
}

// ListNodeWithHost will return a mapping of currently available nodes with the
// host they're mapped to
func (e *EtcdStorage) ListNodeWithHost() (map[string]string, error) {
	nodeToHost := make(map[string]string)
	resp, err := e.client.Keys(EtcdKeyJoin(NODE_PREFIX))
	if err != nil {
		return nil, err
	}
	allHostsMap, err := e.GetVhosts()
	if err != nil {
		return nil, err
	}

	hosts := make([]string, 0, len(allHostsMap))
	for host := range allHostsMap {
		hosts = append(hosts, host)
	}

	hash := e.getConsistentHash(hosts)

	for _, path := range resp {
		split := strings.Split(path, "/")
		node := split[len(split)-1] // node is the last element in the 'path'

		nodeToHost[node], err = e.nodeToHost(node, hash)
		if err != nil {
			return nil, err
		}
	}
	return nodeToHost, nil
}

// NodeToHost maps the node to the correct host according to the internal hash ring
func (e *EtcdStorage) NodeToHost(node string) (string, error) {
	hostMap, err := e.GetVhosts()
	if err != nil {
		return "", err
	}

	hosts := make([]string, 0, len(hostMap))
	for host := range hostMap {
		hosts = append(hosts, host)
	}

	hash := e.getConsistentHash(hosts)
	return e.nodeToHost(node, hash)
}

func (e *EtcdStorage) nodeToHost(node string, hash *consistent.Consistent) (string, error) {

	host, err := hash.Get(node) // gets the host closest to the node in the hash
	if err != nil {
		return "", err
	}

	return host, nil
}

func (e *EtcdStorage) getConsistentHash(hosts []string) *consistent.Consistent {
	consistent := consistent.New()

	for _, host := range hosts {
		consistent.Add(host)
	}

	return consistent
}

// NotifyPersist will update the cluster appropriately according to the given action
func (e *EtcdStorage) NotifyPersist(record interface{}, action int) error {
	var err error
	switch action {
	case ACTION_PUT:
		err = e.putNode(record)
	case ACTION_MULTIPUT:
		err = e.putNodes(record)
	case ACTION_DEL:
		err = e.delNode(record)
	case ACTION_MULTIDEL:
		err = e.delNodes(record)
	}
	if err != nil {
		plog.Error(err)
	}
	return err
}

// PersistWatcher watches for events from etcd, sending events to the given channel.
// If the goconserver instance is leader, it will forward events and also keep the
// nodes.json file in sync with the events from etcd.
func (e *EtcdStorage) PersistWatcher(c chan<- interface{}) {
	key := EtcdKeyJoin(NODE_PREFIX)
	fc := func(events []*clientv3.Event, c chan<- interface{}) {
		put := make([]Node, 0)
		del := make([]string, 0)
		for _, event := range events {
			switch event.Type {
			// put
			case 0:
				node, err := UnmarshalNode(event.Kv.Value)
				if err != nil {
					plog.Warn(err.Error())
					continue
				}
				c <- NewEventData(ACTION_PUT, node)
				put = append(put, *node)
			// del
			case 1:
				temp := strings.Split(string(event.Kv.Key), "/")
				name := temp[len(temp)-1]
				c <- NewEventData(ACTION_DEL, name)
				del = append(del, name)
			}
		}

		e.leaderLock.RLock()
		if e.leader {
			e.leaderLock.RUnlock()
			if len(put) > 0 {
				plog.Info(fmt.Sprintf("Putting new nodes into file: %s", strings.Join(getNodeNamesFromArray(put), ", ")))
				e.nodesFile.MultiPut(put)
			}
			if len(del) > 0 {
				plog.Info(fmt.Sprintf("Deleting nodes from nodes file: %s", strings.Join(del, ", ")))
				e.nodesFile.MultiDel(del)
			}

			e.Sync()
		} else {
			e.leaderLock.RUnlock()
		}
	}
	e.client.Watch(key, fc, c)
}

// SupportWatcher returns true as EtcdStorage watches for events from the etcd cluster
func (e *EtcdStorage) SupportWatcher() bool {
	return true
}

func (e *EtcdStorage) putNode(record interface{}) error {
	var ok bool
	var node *Node
	if node, ok = record.(*Node); !ok {
		return common.ErrInvalidType
	}
	key := EtcdKeyJoin(NODE_PREFIX, node.Name)
	b, err := json.Marshal(*node)
	if err != nil {
		plog.ErrorNode(node.Name, err)
		return err
	}
	err = e.client.Put(key, b)
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdStorage) putNodes(record interface{}) error {
	var ok bool
	var nodes []Node
	if nodes, ok = record.([]Node); !ok {
		return common.ErrInvalidType
	}
	data := make(map[string]string)
	for _, node := range nodes {
		b, err := json.Marshal(node)
		if err != nil {
			plog.ErrorNode(node.Name, err)
			return err
		}
		key := EtcdKeyJoin(NODE_PREFIX, node.Name)
		data[key] = string(b)
	}
	err := e.client.MultiPut(data)
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdStorage) delNode(record interface{}) error {
	var ok bool
	var name string
	if name, ok = record.(string); !ok {
		return common.ErrInvalidType
	}
	key := EtcdKeyJoin(NODE_PREFIX, name)
	err := e.client.Del(key)
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdStorage) delNodes(record interface{}) error {
	var ok bool
	var names []string
	if names, ok = record.([]string); !ok {
		return common.ErrInvalidType
	}
	keys := make([]string, len(names))
	i := 0
	for _, name := range names {
		keys[i] = EtcdKeyJoin(NODE_PREFIX, name)
		i++
	}
	err := e.client.MultiDel(keys)
	if err != nil {
		return err
	}
	return nil
}

// HandlesNode checks if the node is mapped to the host in the internal hash ring
func (e *EtcdStorage) HandlesNode(host, node string) (bool, error) {
	nodeHost, err := e.NodeToHost(node)
	if err != nil {
		return false, err
	}
	return nodeHost == host, nil
}

// Sync ensures that etcd is in compliance with the contents of the nodes.json file
func (e *EtcdStorage) Sync() {
	fileNodes := make(map[string]*Node)
	e.nodesFile.Load(&fileNodes)

	etcdNodes := make(map[string]*Node)
	e.ImportInto(etcdNodes)

	del, put := diff(fileNodes, etcdNodes)

	if len(del) != 0 {
		delNames := getNodeNamesFromMap(del)

		plog.Info(fmt.Sprintf("Deleting nodes in the cluster but not in nodes.json: %s", strings.Join(delNames, ", ")))
		err := e.delNodes(delNames)
		if err != nil {
			plog.Error(err)
		}
	}

	if len(put) != 0 {
		putNodes := getNodesFromMap(put)

		plog.Info(fmt.Sprintf("Creating nodes that were present in nodes.json, but not in etcd: %s", strings.Join(getNodeNamesFromArray(putNodes), ", ")))
		err := e.putNodes(putNodes)
		if err != nil {
			plog.Error(err)
		}
	}
}

// diff gets the nodes you must delete or add to etcd to make it consistent with the file.
func diff(fileNodes map[string]*Node, etcdNodes map[string]*Node) (del map[string]*Node, put map[string]*Node) {
	combined := combine(fileNodes, etcdNodes)

	del = make(map[string]*Node)
	put = make(map[string]*Node)

	for name, node := range combined {
		_, inFile := fileNodes[name]
		_, inEtcd := etcdNodes[name]

		if inEtcd && !inFile {
			del[name] = node
		}

		if inFile && !inEtcd {
			put[name] = node
		}
	}

	return
}

func combine(map1 map[string]*Node, map2 map[string]*Node) map[string]*Node {
	out := make(map[string]*Node)
	for key, val := range map1 {
		out[key] = val
	}

	for key, val := range map2 {
		// if out doesn't have a value at key
		if _, ok := out[key]; !ok {
			out[key] = val
		}
	}

	return out
}

func getNodeNamesFromMap(m map[string]*Node) []string {
	keys := make([]string, len(m))

	i := 0
	for key := range m {
		keys[i] = key
		i++
	}

	return keys
}

func getNodesFromMap(m map[string]*Node) []Node {
	out := make([]Node, len(m))

	i := 0
	for _, val := range m {
		out[i] = *val
		i++
	}

	return out
}

func getNodeNamesFromArray(m []Node) []string {
	out := make([]string, len(m))

	for i, node := range m {
		out[i] = node.Name
	}

	return out
}

package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/xcat2/goconserver/common"
)

func NewNodesFile() NodesFile {
	return &nodesFile{
		lock: &sync.RWMutex{},
	}
}

type NodesFile interface {
	Save(map[string]*Node)
	Load(*map[string]*Node)
	Put(Node)
	MultiPut([]Node)
	Del(string)
	MultiDel([]string)
	Exists() (bool, error)
	Create() error
}

type nodesFile struct {
	lock *sync.RWMutex
}

// Exists checks if the nodes file exists
func (n *nodesFile) Exists() (bool, error) {
	_, err := os.Stat(path.Join(serverConfig.Console.DataDir, "nodes.json"))
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// Create creates the nodes.json file, and any data dirs it needs to along the way
func (n *nodesFile) Create() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.create()
}

func (n *nodesFile) create() error {
	err := os.MkdirAll(serverConfig.Console.DataDir, 0700)
	if err != nil {
		return err
	}

	fmt.Println(path.Join(serverConfig.Console.DataDir, "nodes.json"))

	f, err := os.Create(path.Join(serverConfig.Console.DataDir, "nodes.json"))
	if err != nil {
		return err
	}
	f.Write([]byte{})
	defer f.Close()
	return nil
}

// Save overwrites the content of the file with the given nodes
func (n *nodesFile) Save(nodes map[string]*Node) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.save(nodes)
}

func (n *nodesFile) save(nodes map[string]*Node) {
	exists, err := n.Exists()
	if err != nil {
		plog.Error(fmt.Sprintf("Could not check if nodes.json exists: %s", err.Error()))
		panic(err)
	}
	if exists == false {
		err := n.create()
		if err != nil {
			plog.Error(fmt.Sprintf("Could not create nodes.json file: %s", err.Error()))
			panic(err)
		}
	}

	var data []byte
	if data, err = json.Marshal(nodes); err != nil {
		plog.Error(fmt.Sprintf("Could not Marshal the node map: %s.", err))
		panic(err)
	}
	nodeConfigFile = path.Join(serverConfig.Console.DataDir, "nodes.json")
	nodeBackupFile = path.Join(serverConfig.Console.DataDir, "nodes.json.bak")
	if ok, _ := common.PathExists(nodeConfigFile); ok {
		// TODO: Use rename instead of copy
		_, err = common.CopyFile(nodeBackupFile, nodeConfigFile)
		if err != nil {
			plog.Error(fmt.Sprintf("Unexpected error: %s, exit.", err))
			panic(err)
		}
	}
	err = common.WriteJsonFile(nodeConfigFile, data)
	if err != nil {
		plog.Error(fmt.Sprintf("Unexpected error: %s, exit.", err))
		panic(err)
	}
	go func() {
		_, err = common.CopyFile(nodeBackupFile, nodeConfigFile)
		if err != nil {
			plog.Error(fmt.Sprintf("Unexpected error: %s, exit.", err))
		}
	}()
}

func (n *nodesFile) Load(m *map[string]*Node) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	n.load(m)
}

func (n *nodesFile) load(m *map[string]*Node) {
	nodeConfigFile = path.Join(serverConfig.Console.DataDir, "nodes.json")
	useBackup := false
	if ok, _ := common.PathExists(nodeConfigFile); ok {
		bytes, err := ioutil.ReadFile(nodeConfigFile)
		if err != nil {
			plog.Error(fmt.Sprintf("Could not read node configration file %s.", nodeConfigFile))
			useBackup = true
		}
		if err := json.Unmarshal(bytes, m); err != nil {
			plog.Error(fmt.Sprintf("Could not parse node configration file %s.", nodeConfigFile))
			useBackup = true
		}
	} else {
		useBackup = true
	}
	if !useBackup {
		return
	}
	nodeBackupFile = path.Join(serverConfig.Console.DataDir, "nodes.json.bak")
	if ok, _ := common.PathExists(nodeBackupFile); ok {
		plog.Info(fmt.Sprintf("Trying to load node bakup file %s.", nodeBackupFile))
		bytes, err := ioutil.ReadFile(nodeBackupFile)
		if err != nil {
			plog.Error(fmt.Sprintf("Could not read node backup file %s.", nodeBackupFile))
			return
		}
		if err := json.Unmarshal(bytes, m); err != nil {
			plog.Error(fmt.Sprintf("Could not parse node backup file %s.", nodeBackupFile))
			return
		}
		go func() {
			// as primary file can not be loaded, copy it from backup file
			// TODO: use rename instead of copy
			_, err = common.CopyFile(nodeConfigFile, nodeBackupFile)
			if err != nil {
				plog.Error(fmt.Sprintf("Unexpected error: %s, exit.", err))
				panic(err)
			}
		}()
	}
}

func (n *nodesFile) Put(node Node) {
	n.MultiPut([]Node{node})
}

func (n *nodesFile) MultiPut(nodes []Node) {
	n.lock.Lock()
	defer n.lock.Unlock()
	fmt.Println("got lock...")
	fileContent := make(map[string]*Node)
	n.load(&fileContent)

	for _, node := range nodes {
		fileContent[node.Name] = &node
	}

	n.save(fileContent)
}

func (n *nodesFile) Del(name string) {
	n.MultiDel([]string{name})
}

func (n *nodesFile) MultiDel(names []string) {
	n.lock.Lock()
	defer n.lock.Unlock()
	fileContent := make(map[string]*Node)
	n.load(&fileContent)

	for _, name := range names {
		if _, ok := fileContent[name]; ok {
			delete(fileContent, name)
		}
	}

	n.save(fileContent)
}

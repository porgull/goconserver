package storage

import (
	"reflect"

	"github.com/xcat2/goconserver/common"
)

const (
	STORAGE_FILE = "file"
)

var (
	nodeConfigFile string
	nodeBackupFile string
)

func init() {
	STORAGE_INIT_MAP[STORAGE_FILE] = newFileStorage
}

type FileStorage struct {
	*Storage
	persistence uint32 // 0 no pending data, 1 has pending data
	pending     chan bool
	file        NodesFile
}

func newFileStorage() StorInterface {
	stor := new(Storage)
	stor.async = false
	stor.Nodes = make(map[string]*Node)
	fileStor := new(FileStorage)
	fileStor.Storage = stor
	fileStor.persistence = 0
	fileStor.pending = make(chan bool, 1) // make it non-block
	fileStor.file = NewNodesFile()
	return fileStor
}

func (self *FileStorage) ImportNodes() {
	self.file.Load(&self.Nodes)
}

func (self *FileStorage) NotifyPersist(nodes interface{}, action int) error {
	if action != ACTION_NIL {
		plog.Error(common.ErrUnsupported)
	}
	if reflect.TypeOf(nodes).Kind() == reflect.Map {
		self.Nodes = nodes.(map[string]*Node)
		common.Notify(self.pending, &self.persistence, 1)
	} else {
		plog.Error("Undefine persistance type")
	}
	return nil
}

// a separate thread to save the data, avoid of frequent IO
func (self *FileStorage) PersistWatcher(eventChan chan<- interface{}) {
	common.Wait(self.pending, &self.persistence, 0, self.save)
}

func (self *FileStorage) save() {
	self.file.Save(self.Nodes)
}

func (self *FileStorage) SupportWatcher() bool {
	return false
}

func (self *FileStorage) ListNodeWithHost() (map[string]string, error) {
	return nil, common.ErrUnsupported
}

func (self *FileStorage) GetVhosts() (map[string]*EndpointConfig, error) {
	return nil, common.ErrUnsupported
}

func (self *FileStorage) GetEndpoint(host string) (*EndpointConfig, error) {
	return nil, common.ErrUnsupported
}

func (self *FileStorage) HandlesNode(host, node string) (bool, error) {
	return true, nil // FileStorage means only one instance, which handles all nodes
}

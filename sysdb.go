package sysdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jinzhu/copier"
	bolt "go.etcd.io/bbolt"
)

const (
	EmptyObj   = "{}"
	EmptyArray = "[]"
)

// Bucket which stores all raw events
// and the meta data of the streams
const (
	EventDesc = "#eventdesc"
	Events    = "#events"
)

// Integer to bytes
func i64tob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Bytes to Integer
func btoi64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

// Uint to bytes
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Bytes to UInteger
func btou64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

// Stream Path
func streamPath() string {
	return "/" + EventDesc
}

type EventType int64

// The data base is hierarchical store, like an window registry
// each path, has a set of keys
// each key has a value, which is defined by a type. type is specified by json schema,
// The json blob is unparsed/parsed to/from the data structure

// Every data structure should implement this interface
type Obj interface {
	Save() (string, error)
	Load(bytes []byte) error
}

// Obj2Map converts Obj to map of interfaces
func Obj2Map(o Obj) map[string]interface{} {
	s, err := o.Save()
	if err != nil {
		panic("Bug in object serialization")
	}
	result := map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &result)
	if err != nil {
		panic("Bug in object serialization")
	}
	return result
}

// Event handler which is called on any CRUD operation on the key
type EvtHandler interface {
	Event(path string, key string, evt EventType, value Obj, status error)
}

// map from key to type of the object
type ObjType map[string]reflect.Type

// for each path, store the key to type map. for eg. each path can
// multiple keys
type PathTypes map[string]ObjType

// Event handlers for each path
type EvtHandlers map[string][]EvtHandler

// List of event types
const (
	EVT_UNDEFINED EventType = iota
	EVT_OBJ_PRE_SET
	EVT_OBJ_SET
	EVT_OBJ_PRE_UPD
	EVT_OBJ_UPD
	EVT_OBJ_PRE_DEL
	EVT_OBJ_DEL
)

// database interface for storing the key value pairs
type Sysdb interface {
	Path() string
	Backup(fName string) error
	Close()

	CreateBktPath(bktPath string) error
	ExistsBkt(bktPath string) bool
	Buckets(keyPath string) []string
	DeleteBkt(bktPath string) error

	SetKeyValue(path string, key string, obj Obj) error
	Put(path string, key string, obj Obj) error

	GetKeyValue(path string, key string) (Obj, error)
	Get(path string, key string) (Obj, error)

	DelKey(path string, key string) error
	Delete(path string, key string) error

	RegisterObj(path string, key string, typ reflect.Type)
	RegisterEvtHandler(path string, key string, evtHandler EvtHandler)

	MergeAndSave(path string, key string, newv Obj) error

	CreateStream(name string, typ reflect.Type) error
	AppendStream(name string, obj Obj) error
	ReadStream(name string, from int64, to int64) ([]Obj, error)
}

func (o PathTypes) RegisterKeyType(path string, key string, typ reflect.Type) {

	for curpath := range o {
		if curpath == path {
			o[curpath][key] = typ
			return
		}
	}
	// no match
	o[path] = make(ObjType, 0)
	o[path][key] = typ
}

func (o PathTypes) GetKeyType(path string, key string) reflect.Type {

	for pattern := range o {
		match, e := regexp.MatchString(pattern, path)
		if e != nil {
			Logger().Debug("Key Match error path %v key %v error %v", path, key, e)
		}
		if match {
			if _, ok := o[pattern][key]; ok {
				return o[pattern][key]
			} else {
				Logger().Error("Key not found path %v pattern %v key %v", path, pattern, key)
			}
		}
	}
	Logger().Error("Key search failed path %v key %v", path, key)
	return nil
}

func (o EvtHandlers) RegisterEvtHandler(path string, key string, evtHandler EvtHandler) {
	path = path + "/" + key
	if _, ok := o[path]; !ok {
		o[path] = make([]EvtHandler, 0)
	}
	o[path] = append(o[path], evtHandler)
}

func (o EvtHandlers) FireHandlers(path string, key string, evt EventType, value Obj, status error) {

	path = path + "/" + key
	if handlers, ok := o[path]; ok {
		for _, v := range handlers {
			v.Event(path, key, evt, value, status)
		}
	}
}

type sysdb struct {
	db          *bolt.DB
	pathTypes   PathTypes
	evtHandlers EvtHandlers
}

func NewSysDb(filePath string) Sysdb {

	db, err := bolt.Open(filePath, 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &sysdb{db: db, pathTypes: make(PathTypes, 10),
		evtHandlers: make(EvtHandlers, 10)}
}

func (s *sysdb) Path() string {
	return s.db.Path()
}

func (s *sysdb) Close() {
	s.db.Close()
}

func (s *sysdb) Backup(fName string) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(fName, 0666)
	})
}

func (s *sysdb) RegisterObj(path string, key string, typ reflect.Type) {
	Logger().Debug("Object registration path %v, key %v , %v", path, key, typ)
	s.pathTypes.RegisterKeyType(path, key, typ)
}

func (s *sysdb) RegisterEvtHandler(path string, key string, evtHandler EvtHandler) {
	s.evtHandlers.RegisterEvtHandler(path, key, evtHandler)
}

func (s *sysdb) CreateBktPath(bktPath string) error {

	err := s.db.Update(func(tx *bolt.Tx) error {
		parts := strings.Split(bktPath, "/")
		var bkt *bolt.Bucket
		var err error
		for _, bktName := range parts[1:] {
			if bkt == nil {
				bkt, err = tx.CreateBucketIfNotExists([]byte(bktName))
			} else {
				bkt, err = bkt.CreateBucketIfNotExists([]byte(bktName))
			}
			if err != nil {
				return fmt.Errorf("error creating resource: %s", bktName)
			}
		}
		return nil
	})
	return err
}

func (s *sysdb) ExistsBkt(bktPath string) bool {

	var exists bool = false
	s.db.View(func(tx *bolt.Tx) error {
		var bkt *bolt.Bucket
		parts := strings.Split(bktPath, "/")
		for _, bktName := range parts[1:] {
			if bkt == nil {
				bkt = tx.Bucket([]byte(bktName))
			} else {
				bkt = bkt.Bucket([]byte(bktName))
			}
			if bkt == nil {
				return fmt.Errorf("resource %s doesn't exist", bktName)
			}
		}
		if bkt != nil {
			exists = true
		}
		return nil
	})
	return exists
}

func (s *sysdb) DeleteBkt(bktPath string) error {

	err := s.db.Update(func(tx *bolt.Tx) error {
		var bkt *bolt.Bucket
		parts := strings.Split(bktPath, "/")
		if len(parts) < 2 {
			panic(fmt.Sprintf("Deleting root resource %s", parts[0]))
		}
		for _, bktName := range parts[1 : len(parts)-1] {
			if bkt == nil {
				bkt = tx.Bucket([]byte(bktName))
			} else {
				bkt = bkt.Bucket([]byte(bktName))
			}
			if bkt == nil {
				return fmt.Errorf("resource %s doesn't exist", bktName)
			}
		}
		err := bkt.DeleteBucket([]byte(parts[len(parts)-1]))
		return err
	})
	return err
}

func (s *sysdb) SetKeyValue(path string, key string, obj Obj) error {

	err := s.db.Update(func(tx *bolt.Tx) error {
		parts := strings.Split(path, "/")
		var bkt *bolt.Bucket
		for _, bktName := range parts[1:] {
			if bkt == nil {
				bkt = tx.Bucket([]byte(bktName))
			} else {
				bkt = bkt.Bucket([]byte(bktName))
			}
			if bkt == nil {
				return fmt.Errorf("resource %s doesn't exist", bktName)
			}
		}
		value, _ := obj.Save()
		Logger().Debug("db saving path %s key %s value %s", path, key, value)
		return bkt.Put([]byte(key), []byte(value))
	})
	return err
}

func (s *sysdb) GetKeyValue(path string, key string) (Obj, error) {

	var value Obj
	err := s.db.Update(func(tx *bolt.Tx) error {
		parts := strings.Split(path, "/")
		var bkt *bolt.Bucket
		for _, bktName := range parts[1:] {
			if bkt == nil {
				bkt = tx.Bucket([]byte(bktName))
			} else {
				bkt = bkt.Bucket([]byte(bktName))
			}
			if bkt == nil {
				return fmt.Errorf("resource %s doesn't exist", bktName)
			}
		}
		bytes := bkt.Get([]byte(key))

		// Find object type from the path database and create an  empty object of that type
		t := s.pathTypes.GetKeyType(path, key)
		if bytes == nil {
			switch t.Kind() {
			case reflect.Array, reflect.Slice:
				bytes = []byte(EmptyArray)
			default:
				bytes = []byte(EmptyObj)
			}
		}
		v := reflect.New(t).Interface()
		value = v.(Obj)
		// Construct the object in memory now, This allows us to use the type system of golang
		// with out duck typing any object
		err := value.Load(bytes)
		return err
	})
	return value, err
}

func (s *sysdb) DelKey(path string, key string) error {

	err := s.db.Update(func(tx *bolt.Tx) error {
		parts := strings.Split(path, "/")
		var bkt *bolt.Bucket
		for _, bktName := range parts[1:] {
			if bkt == nil {
				bkt = tx.Bucket([]byte(bktName))
			} else {
				bkt = bkt.Bucket([]byte(bktName))
			}
			if bkt == nil {
				return fmt.Errorf("resource %s doesn't exist", bktName)
			}
		}
		bkt.Delete([]byte(key))
		return nil
	})
	return err
}

func (s *sysdb) MergeAndSave(path string, key string, newv Obj) error {
	//merge the values
	oldv, err := s.GetKeyValue(path, key)

	if err != nil {
		return fmt.Errorf("invalid object value for path %s %s", path, key)
	}

	s.evtHandlers.FireHandlers(path, key, EVT_OBJ_PRE_UPD, oldv, nil)
	err = copier.Copy(oldv, newv)

	if err != nil {
		return fmt.Errorf("invalid input object value for path %s %v", path, newv)
	}
	err = s.SetKeyValue(path, key, oldv)
	s.evtHandlers.FireHandlers(path, key, EVT_OBJ_UPD, oldv, nil)
	return err
}

func (s *sysdb) Put(path string, key string, obj Obj) error {
	s.evtHandlers.FireHandlers(path, key, EVT_OBJ_PRE_SET, obj, nil)
	err := s.SetKeyValue(path, key, obj)
	s.evtHandlers.FireHandlers(path, key, EVT_OBJ_SET, obj, err)
	return err
}

func (s *sysdb) Delete(path string, key string) error {
	obj, _ := s.GetKeyValue(path, key)
	s.evtHandlers.FireHandlers(path, key, EVT_OBJ_PRE_DEL, obj, nil)
	err := s.DelKey(path, key)
	s.evtHandlers.FireHandlers(path, key, EVT_OBJ_DEL, obj, err)
	return err
}

func (s *sysdb) Get(path string, key string) (Obj, error) {
	obj, err := s.GetKeyValue(path, key)
	return obj, err
}

// return list of all child buckets.
func (s *sysdb) Buckets(bktPath string) []string {

	bkts := []string{}
	s.db.View(func(tx *bolt.Tx) error {
		var bkt *bolt.Bucket
		parts := strings.Split(bktPath, "/")
		for _, bktName := range parts[1:] {
			if len(bktName) == 0 {
				continue
			}
			if bkt == nil {
				bkt = tx.Bucket([]byte(bktName))
			} else {
				bkt = bkt.Bucket([]byte(bktName))
			}
			if bkt == nil {
				return fmt.Errorf("resource %s doesn't exist", bktName)
			}
		}
		if bkt != nil {
			bkt.ForEach(func(name []byte, v []byte) error {
				bkts = append(bkts, string(name))
				return nil
			})
		} else {
			tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				bkts = append(bkts, string(name))
				return nil
			})
		}
		return nil
	})
	return bkts
}

// Streams Database

// Stores the key value pairs, which maps timestamp to eventid
// for each stream

/* /$eventdesc/<stream-name>/<keyid>
*
* keyid                        objtype
* timestamp                    eventid, unique sequence number
 */

// global sequence of events
/* /$events/<keyid>
*
* keyid                      objtype
* eventid                    Json Object, currently api.Alert{}
 */

func (s *sysdb) CreateStream(name string, typ reflect.Type) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(EventDesc))
		Logger().Info("Created Bucket %v key %v", bucket, EventDesc)
		if err != nil {
			return err
		}
		_, err = bucket.CreateBucketIfNotExists([]byte(name))

		if err != nil {
			return err
		}
		Logger().Info("Created Bucket %v key %v", bucket, name)
		_, err = tx.CreateBucketIfNotExists([]byte(Events))
		return err
	})
	if err == nil {
		s.RegisterObj(streamPath(), name, typ)
	}
	Logger().Info("Registered key %v %T", name, typ)
	return err
}

func (s *sysdb) AppendStream(name string, obj Obj) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(EventDesc))
		if bkt == nil {
			return fmt.Errorf("meta data resource %s doesn't exist", EventDesc)
		}
		stream := bkt.Bucket([]byte(name))
		if stream == nil {
			return fmt.Errorf("stream resource %s doesn't exist", name)
		}
		bktdata := tx.Bucket([]byte(Events))
		if bkt == nil {
			return fmt.Errorf("event data resource %s doesn't exist", Events)
		}
		id, err := bkt.NextSequence()
		if err != nil {
			return err
		}
		sequence := u64tob(id)
		ev, _ := obj.Save()
		err = bktdata.Put(sequence, []byte(ev))
		if err != nil {
			return err
		}
		timestamp := time.Now().UnixNano()
		streamId := i64tob(timestamp)
		err = stream.Put(streamId, sequence)
		return err
	})
	return err
}

func (s *sysdb) ReadStream(name string, from int64, to int64) ([]Obj, error) {
	events := []Obj{}

	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(EventDesc))
		if bkt == nil {
			return fmt.Errorf("meta data resource %s doesn't exist", EventDesc)
		}
		stream := bkt.Bucket([]byte(name))
		if stream == nil {
			return fmt.Errorf("stream resource %s doesn't exist", name)
		}
		bktdata := tx.Bucket([]byte(Events))
		if bkt == nil {
			return fmt.Errorf("event data resource %s doesn't exist", Events)
		}
		c := stream.Cursor()

		t := s.pathTypes.GetKeyType(streamPath(), name)
		var err error
		for k, v := c.First(); k != nil; k, v = c.Next() {

			timestamp := btoi64(k)
			if timestamp >= from && timestamp <= to {
				bytes := bktdata.Get(v)
				v := reflect.New(t).Interface()
				value := v.(Obj)
				err = value.Load(bytes)
				Logger().Debug("STREAMS EVENT %s", string(bytes))
				Logger().Debug("STREAMS EVENT %v", value)
				if err == nil {
					events = append(events, value)
				}
			}
		}
		return err
	})
	return events, err
}

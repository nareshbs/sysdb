package tests

import (
	"os"
	"reflect"
	"sysdb"
	"testing"

	"github.com/stretchr/testify/assert"
)

var g_sysdb sysdb.Sysdb

const (
	dbFilename       = "testsysdb.db"
	bktRootName      = "root"
	bktPathExists    = "/root/apc/exists"
	bktPathPattern   = "/root/apc*"
	bktPathNotExists = "/root/notexists"
	keyMsg           = "Msg"
	keyNode          = "Node"
	keyServices      = "services"
	path             = bktPathExists
)

var (
	bktList = []string{"/root", "/root/bkt-1", "/root/bkt-2"}
)

func setupDatabase(t *testing.T) {
	t.Logf("[SETUP] Hello !")
	g_sysdb = sysdb.NewSysDb(dbFilename)
}

func teardownDatabase(t *testing.T) {
	t.Logf("[TEARDOWN] Bye, bye !")
	_ = os.Remove(dbFilename)
}

func initDatabase(t *testing.T) {
	t.Logf("[INIT] Database !")
	err := g_sysdb.CreateBktPath(bktPathExists)

	assert.Equal(t, nil, err)
	g_sysdb.RegisterObj(bktPathPattern, keyMsg, reflect.TypeOf(Msg{}))
	g_sysdb.RegisterObj(bktPathPattern, keyNode, reflect.TypeOf(Node{}))

	g_sysdb.RegisterEvtHandler(path, keyMsg, OnEvent_Msg{})
	g_sysdb.RegisterEvtHandler(path, keyNode, OnEvent_Node{})
}

func TestDbSetup(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	dbFile := g_sysdb.Path()
	if dbFile != dbFilename {
		t.Errorf("Expected %v,  got %v", dbFile, dbFilename)
	}
}

func TestDbCreateBucket(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	err := g_sysdb.CreateBktPath(bktPathExists)
	assert.Equal(t, nil, err)
	exists := g_sysdb.ExistsBkt(bktPathExists)
	if exists != true {
		t.Errorf("Expected %v,  got %v", true, exists)
	}
}

func TestDbNonExistBkt(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	err := g_sysdb.CreateBktPath(bktPathExists)
	assert.Equal(t, nil, err)
	exists := g_sysdb.ExistsBkt(bktPathNotExists)
	if exists != false {
		t.Errorf("Expected %v,  got %v", true, exists)
	}
}

func TestDbListBuckets(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	err := g_sysdb.CreateBktPath(bktPathExists)
	assert.Equal(t, nil, err)
	bkts := g_sysdb.Buckets("/")
	expBkts := [...]string{bktRootName}
	assert.ElementsMatch(t, expBkts, bkts)
}

func TestDbListMultipleBuckets(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	for _, bktName := range bktList {
		err := g_sysdb.CreateBktPath(bktName)
		assert.Equal(t, nil, err)
	}
	bkts := g_sysdb.Buckets("/root")
	expBkts := [...]string{"bkt-1", "bkt-2"}
	assert.ElementsMatch(t, expBkts, bkts)
}

func TestDbSetKeyValue(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	initDatabase(t)

	val := &Msg{
		Code:    10,
		Message: "test message",
	}
	err := g_sysdb.Put(path, keyMsg, val)
	assert.Equal(t, err, nil)
	o, err := g_sysdb.GetKeyValue(path, keyMsg)
	val = o.(*Msg)
	assert.Equal(t, err, nil)
	assert.Equal(t, o, val)
}

func TestDbMergeAndSave(t *testing.T) {
	setupDatabase(t)
	defer teardownDatabase(t)
	initDatabase(t)

	val := &Node{
		Id:     10,
		Desc:   "test message",
		Uptime: 1024,
	}
	err := g_sysdb.Put(path, keyNode, val)
	assert.Equal(t, nil, err)
	o, err := g_sysdb.GetKeyValue(path, keyNode)
	val = o.(*Node)
	assert.Equal(t, nil, err)
	assert.Equal(t, o, val)

	val.Desc = "new test message"
	val.Uptime = 2048
	err = g_sysdb.MergeAndSave(path, keyNode, val)
	assert.Equal(t, nil, err)
	o, err = g_sysdb.GetKeyValue(path, keyNode)
	val = o.(*Node)
	assert.Equal(t, nil, err)
	assert.Equal(t, o, val)

}

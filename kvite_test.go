package kvite

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	log "github.com/Sirupsen/logrus"
	logx "github.com/mistifyio/mistify-logrus-ext"
	"github.com/stretchr/testify/suite"
)

type KViteTestSuite struct {
	suite.Suite
	DB      *DB
	TempDir string
}

func (s *KViteTestSuite) SetupTest() {
	// Create a new db tempdir
	dir, err := ioutil.TempDir("", "kvite-")
	s.NoError(err)
	s.TempDir = dir

	// Open the db with the tempdir
	db, err := Open(filepath.Join(s.TempDir, "kvite.db"), "testing")
	s.NoError(err)
	s.DB = db
}

func (s *KViteTestSuite) TearDownTest() {
	// Close the db
	logx.LogReturnedErr(s.DB.Close, nil, "failed to close database")

	// Clean up the db tempdir
	logx.LogReturnedErr(func() error { return os.RemoveAll(s.TempDir) },
		log.Fields{"tempdir": s.TempDir}, "failed to remove tempdir")
}

func TestKViteTestSuite(t *testing.T) {
	suite.Run(t, new(KViteTestSuite))
}

func (s *KViteTestSuite) TestDBOpen() {
	// The suite test setup tests a good call to the kvite.Open function
	s.Equal("testing", s.DB.table)
}

func (s *KViteTestSuite) TestDBClose() {
	// The suite test setup tests a good call to the kvite.Close function
	// Attempt to close again
	s.NoError(s.DB.Close())
}

func (s *KViteTestSuite) TestDBBegin() {
	tx, err := s.DB.Begin()
	s.NoError(err)
	s.NotNil(tx)
}

func (s *KViteTestSuite) TestTXRollback() {
	tx, _ := s.DB.Begin()
	s.NoError(tx.Rollback())
	s.Error(tx.Rollback())
}

func (s *KViteTestSuite) TestTXCommit() {
	tx, _ := s.DB.Begin()
	s.NoError(tx.Commit())
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestCreateBucket() {
	tx, _ := s.DB.Begin()
	b, err := tx.CreateBucket("test")
	s.NoError(err)
	s.NotNil(b)
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestCreateBucketIfNotExists() {
	tx, _ := s.DB.Begin()
	b, err := tx.CreateBucketIfNotExists("test")
	s.NoError(err)
	s.NotNil(b)
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestPut() {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")

	s.NoError(b.Put("foo", []byte("bar")))
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestGet() {
	tx, err := s.DB.Begin()
	b, err := tx.CreateBucket("test")

	_ = b.Put("foo", []byte("bar"))

	val, err := b.Get("foo")
	s.NoError(err)
	s.Equal("bar", string(val))
}

func (s *KViteTestSuite) TestDelete() {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")
	_ = b.Put("foo", []byte("bar"))

	s.NoError(b.Delete("foo"))

	val, _ := b.Get("foo")
	s.Equal([]byte(nil), val)

	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestTransaction() {
	err := s.DB.Transaction(func(tx *Tx) error {
		b, _ := tx.CreateBucket("test")
		_ = b.Put("foo", []byte("bar"))
		return nil
	})
	s.NoError(err)

	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")
	val, _ := b.Get("foo")
	s.Equal(string(val), "bar")
}

func (s *KViteTestSuite) TestForEach() {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")

	_ = b.Put("foo", []byte("bar"))
	_ = b.Put("baz", []byte("stuff"))

	var items []string
	err := b.ForEach(func(k string, v []byte) error {
		items = append(items, k)
		return nil
	})
	s.NoError(err)
	s.Len(items, 2)
}

func (s *KViteTestSuite) TestBuckets() {
	buckets := []string{"one", "two", "three"}
	_ = s.DB.Transaction(func(tx *Tx) error {
		for _, name := range buckets {
			b, _ := tx.CreateBucket(name)
			_ = b.Put("foo", []byte("bar"))
		}
		return nil
	})

	names, err := s.DB.Buckets()
	s.NoError(err)
	s.Equal(buckets, names)
}

func (s *KViteTestSuite) TestUnique() {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")

	s.NoError(b.Put("foo", []byte("bar")))
	s.NoError(b.Put("foo", []byte("baz")))

	i := 0
	_ = b.ForEach(func(k string, v []byte) error {
		i++
		s.Equal("baz", string(v))
		return nil
	})
	s.Equal(1, i)
}

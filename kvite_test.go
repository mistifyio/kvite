package kvite

import (
	"errors"
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

func (s *KViteTestSuite) testStoredValue(bucketName, key string, expectedValue []byte) {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket(bucketName)
	value, getErr := b.Get(key)
	s.NoError(getErr)
	s.Equal(expectedValue, value)
	_ = tx.Rollback()
}

func TestKViteTestSuite(t *testing.T) {
	suite.Run(t, new(KViteTestSuite))
}

func (s *KViteTestSuite) TestDBOpen() {
	// The suite test setup tests a good call to the kvite.Open function

	tests := []struct {
		filename    string
		table       string
		expectedErr bool
		msg         string
	}{
		{"", "", true, "directory as db file"},
		{"open-test-bad-table.db", "1-23aa'1234", true, "invalid table name"},
		{"open-test-no-table.db", "", false, "no supplied table name"},
	}

	for _, test := range tests {
		_, err := Open(filepath.Join(s.TempDir, test.filename), test.table)
		if test.expectedErr {
			s.Error(err, test.msg)
		} else {
			s.NoError(err, test.msg)
		}
	}
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

func (s *KViteTestSuite) TestTxRollback() {
	tx, _ := s.DB.Begin()
	s.NoError(tx.Rollback())
	// Can't rollback a finished tx
	s.Error(tx.Rollback())
}

func (s *KViteTestSuite) TestTxCommit() {
	tx, _ := s.DB.Begin()
	s.NoError(tx.Commit())
	// Can't commit a finished tx
	s.Error(tx.Commit())
}

func (s *KViteTestSuite) TestTxCreateBucket() {
	tx, _ := s.DB.Begin()
	b, err := tx.CreateBucket("test")
	s.NoError(err)
	s.NotNil(b)
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestTxCreateBucketIfNotExists() {
	tx, _ := s.DB.Begin()
	b, err := tx.CreateBucketIfNotExists("test")
	s.NoError(err)
	s.NotNil(b)
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestBucketPut() {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")

	s.NoError(b.Put("foo", []byte("bar")))
	s.NoError(tx.Commit())
}

func (s *KViteTestSuite) TestBucketGet() {
	bucketName := "test"
	key := "foo"
	value := []byte("bar")

	tx, err := s.DB.Begin()
	b, err := tx.CreateBucket(bucketName)

	_ = b.Put(key, value)

	// Get value inside tx
	val, err := b.Get(key)
	s.NoError(err)
	s.EqualValues(value, val)

	s.NoError(tx.Commit())

	// Get value after tx
	s.testStoredValue(bucketName, key, value)
	// Get non-existent value
	s.testStoredValue(bucketName, "asdf", []byte(nil))
}

func (s *KViteTestSuite) TestBucketDelete() {
	bucketName := "test"
	key := "foo"
	value := []byte("bar")

	// Put and remove in same tx
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket(bucketName)
	_ = b.Put(key, value)
	s.NoError(b.Delete(key))

	// Check removed in tx
	val, _ := b.Get(key)
	s.Equal([]byte(nil), val)

	s.NoError(tx.Commit())

	// Check removed after tx
	s.testStoredValue(bucketName, key, []byte(nil))

	// Put and remove in different tx
	tx, _ = s.DB.Begin()
	b, _ = tx.CreateBucket(bucketName)
	_ = b.Put(key, value)
	tx.Commit()

	tx, _ = s.DB.Begin()
	b, _ = tx.CreateBucket(bucketName)
	s.NoError(b.Delete(key))
	s.NoError(tx.Commit())

	// Check removed after tx
	s.testStoredValue(bucketName, key, []byte(nil))
}

func (s *KViteTestSuite) TestDBTransaction() {
	bucketName := "test"
	key := "foo"
	value := []byte("bar")

	// No error, tx should commit
	err := s.DB.Transaction(func(tx *Tx) error {
		b, _ := tx.CreateBucket(bucketName)
		_ = b.Put(key, value)
		return nil
	})
	s.NoError(err)
	s.testStoredValue(bucketName, key, value)

	// Error, tx should rollback
	err = s.DB.Transaction(func(tx *Tx) error {
		b, _ := tx.CreateBucket(bucketName)
		_ = b.Put(key, []byte("asdf"))

		// Can't commit or rollback inside a db managed tx
		s.Error(tx.Commit())
		s.Error(tx.Rollback())

		return errors.New("an error")
	})
	s.Error(err)
	// Still original set value
	s.testStoredValue(bucketName, key, value)
}

func (s *KViteTestSuite) TestBucketForEach() {
	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket("test")

	_ = b.Put("foo", []byte("bar"))
	_ = b.Put("baz", []byte("stuff"))

	// No error in fn
	var items []string
	err := b.ForEach(func(k string, v []byte) error {
		items = append(items, k)
		return nil
	})
	s.NoError(err)
	s.Len(items, 2)

	// Error in fn
	err = b.ForEach(func(k string, v []byte) error {
		return errors.New("an error")
	})
	s.Error(err)

}

func (s *KViteTestSuite) TestDBBuckets() {
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

func (s *KViteTestSuite) TestBucketPutUnique() {
	bucketName := "test"
	key := "foo"
	value := []byte("bar")

	tx, _ := s.DB.Begin()
	b, _ := tx.CreateBucket(bucketName)

	// Should only be one key set and should be last set value
	s.NoError(b.Put(key, []byte("baz")))
	s.NoError(b.Put(key, value))

	i := 0
	_ = b.ForEach(func(k string, v []byte) error {
		i++
		s.Equal(value, v)
		return nil
	})
	s.Equal(1, i)
}

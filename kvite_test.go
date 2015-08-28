package kvite

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func withDB(t *testing.T, fn func(db *DB, t *testing.T)) {
	file := tempfile()
	db, err := Open(file, "testing")
	ok(t, err)
	defer removeFileAndLogError(file)
	defer logErr(db.Close, "database close")
	fn(db, t)
}

func TestOpen(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
	})
}

func TestBegin(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer logErr(tx.Rollback, "Transaction Rollback")
	})
}

func TestRollback(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		err = tx.Rollback()
		ok(t, err)
	})
}

func TestCommit(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		err = tx.Commit()
		ok(t, err)
	})
}

func TestCreateBucket(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		defer logErr(tx.Rollback, "Transaction Rollback")
		_, err = tx.CreateBucket("test")
		ok(t, err)
		err = tx.Commit()
		ok(t, err)
	})
}

func TestCreateBucketIfNotExists(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		defer logErr(tx.Rollback, "Transaction Rollback")
		_, err = tx.CreateBucketIfNotExists("test")
		ok(t, err)
		err = tx.Commit()
		ok(t, err)
	})
}

func TestPut(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		defer logErr(tx.Rollback, "Transaction Rollback")
		b, err := tx.CreateBucket("test")
		ok(t, err)

		err = b.Put("foo", []byte("bar"))
		ok(t, err)
		err = tx.Commit()
		ok(t, err)
	})
}

func TestGet(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		defer logErr(tx.Rollback, "Transaction Rollback")
		b, err := tx.CreateBucket("test")
		ok(t, err)

		err = b.Put("foo", []byte("bar"))
		ok(t, err)

		val, err := b.Get("foo")
		ok(t, err)

		equals(t, string(val), "bar")

		err = tx.Commit()
		ok(t, err)
	})
}

func TestDelete(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		ok(t, err)
		defer logErr(tx.Rollback, "Transaction Rollback")
		b, err := tx.CreateBucket("test")
		ok(t, err)

		err = b.Put("foo", []byte("bar"))
		ok(t, err)

		val, err := b.Get("foo")
		ok(t, err)

		equals(t, string(val), "bar")

		err = b.Delete("foo")
		ok(t, err)

		val, err = b.Get("foo")
		equals(t, []byte(nil), val)
		ok(t, err)

		err = tx.Commit()
		ok(t, err)
	})
}

func TestTransaction(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			b, err := tx.CreateBucket("test")
			ok(t, err)

			err = b.Put("foo", []byte("bar"))
			ok(t, err)

			val, err := b.Get("foo")
			ok(t, err)

			equals(t, string(val), "bar")

			return nil
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestForEach(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			b, err := tx.CreateBucket("test")
			ok(t, err)

			err = b.Put("foo", []byte("bar"))
			ok(t, err)

			err = b.Put("baz", []byte("stuff"))
			ok(t, err)

			var items []string
			err = b.ForEach(func(k string, v []byte) error {
				items = append(items, k)
				return nil
			})
			ok(t, err)

			if len(items) != 2 {
				return fmt.Errorf("length does not match")
			}

			return nil
		})

		ok(t, err)
	})
}

func TestBuckets(t *testing.T) {
	buckets := []string{"one", "two", "three"}
	withDB(t, func(db *DB, t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			for _, name := range buckets {
				b, err := tx.CreateBucket(name)
				ok(t, err)
				err = b.Put("foo", []byte("bar"))
				ok(t, err)
			}
			return nil
		})

		names, err := db.Buckets()
		ok(t, err)
		equals(t, buckets, names)
	})
}

func TestUnique(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			b, err := tx.CreateBucket("test")
			ok(t, err)

			err = b.Put("foo", []byte("bar"))
			ok(t, err)
			err = b.Put("foo", []byte("baz"))

			err = b.ForEach(func(k string, v []byte) error {
				equals(t, "baz", string(v))
				return nil
			})
			ok(t, err)
			return nil
		})
		ok(t, err)
	})
}

func BenchmarkPutGet(bm *testing.B) {
	file := tempfile()
	db, err := Open(file, "testing")

	if err != nil {
		bm.Fatal(err)
	}
	defer removeFileAndLogError(file)
	defer logErr(db.Close, "database close")

	err = db.Transaction(func(tx *Tx) error {
		b, err := tx.CreateBucket("test")
		if err != nil {
			return err
		}

		for n := 0; n < bm.N; n++ {
			err = b.Put("foo", []byte("bar"))
			if err != nil {
				return err
			}
			_, err := b.Get("foo")
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		bm.Fatal(err)
	}
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "kvite-")
	logErr(f.Close, "temp file close")
	removeFileAndLogError(f.Name())
	return f.Name()
}

// Thanks to https://github.com/benbjohnson/testing

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

func logErr(fn func() error, message string) {
	if err := fn(); err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: Error: %s: %s", filepath.Base(file), line, message, err.Error())
	}
}

func removeFileAndLogError(file string) {
	if err := os.Remove(file); err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("%s:%d: Error removing file '%s': %s", filepath.Base(file), line, file, err.Error())
	}
}

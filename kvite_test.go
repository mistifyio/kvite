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

	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file)
	defer db.Close()

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
		defer tx.Rollback()
	})
}

func TestRollback(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestCommit(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestCreateBucket(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, err = tx.CreateBucket("test")
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestCreateBucketIfNotExists(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, err = tx.CreateBucketIfNotExists("test")
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestPut(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		b, err := tx.CreateBucket("test")
		if err != nil {
			t.Fatal(err)
		}

		err = b.Put("foo", []byte("bar"))
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestGet(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		b, err := tx.CreateBucket("test")
		if err != nil {
			t.Fatal(err)
		}

		err = b.Put("foo", []byte("bar"))
		if err != nil {
			t.Fatal(err)
		}

		val, err := b.Get("foo")
		if err != nil {
			t.Fatal(err)
		}

		if string(val) != "bar" {
			t.Fatalf("values fo not match")
		}

		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDelete(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		b, err := tx.CreateBucket("test")
		if err != nil {
			t.Fatal(err)
		}

		err = b.Put("foo", []byte("bar"))
		if err != nil {
			t.Fatal(err)
		}

		val, err := b.Get("foo")
		if err != nil {
			t.Fatal(err)
		}

		if string(val) != "bar" {
			t.Fatalf("values fo not match")
		}

		err = b.Delete("foo")
		if err != nil {
			t.Fatal(err)
		}

		val, err = b.Get("foo")
		if val != nil {
			t.Fatalf("got a value when should have been nil")
		}

		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestTransaction(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			b, err := tx.CreateBucket("test")
			if err != nil {
				return err
			}

			err = b.Put("foo", []byte("bar"))
			if err != nil {
				return err
			}

			val, err := b.Get("foo")
			if err != nil {
				return err
			}

			if string(val) != "bar" {
				return fmt.Errorf("values do not match")
			}
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
			if err != nil {
				return err
			}

			err = b.Put("foo", []byte("bar"))
			if err != nil {
				return err
			}

			err = b.Put("baz", []byte("stuff"))
			if err != nil {
				return err
			}

			items := make([]string, 0)
			err = b.ForEach(func(k string, v []byte) error {
				items = append(items, k)
				return nil
			})
			if err != nil {
				return err
			}

			if len(items) != 2 {
				return fmt.Errorf("length does not match")
			}

			return nil
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBuckets(t *testing.T) {
	buckets := []string{"one", "two", "three"}
	withDB(t, func(db *DB, t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			for _, name := range buckets {
				b, err := tx.CreateBucket(name)
				if err != nil {
					return err
				}
				err = b.Put("foo", []byte("bar"))
				if err != nil {
					return err
				}
			}
			return nil
		})

		names, err := db.Buckets()
		ok(t, err)
		equals(t, buckets, names)
	})
}

func BenchmarkPutGet(bm *testing.B) {
	file := tempfile()
	db, err := Open(file, "testing")

	if err != nil {
		bm.Fatal(err)
	}
	defer os.Remove(file)
	defer db.Close()

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
	f.Close()
	os.Remove(f.Name())
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

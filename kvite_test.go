package kvite

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func withDB(t *testing.T, fn func(db *DB, t *testing.T)) {
	file := tempfile()
	db, err := Open(file)

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

func TestBucket(t *testing.T) {
	withDB(t, func(db *DB, t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		b, err := tx.Bucket("test")
		if err != nil {
			t.Fatal(err)
		}
		if b != nil {
			t.Fatal("got a bucket when it should have been nil")
		}
		_, err = tx.CreateBucket("test")
		if err != nil {
			t.Fatal(err)
		}
		b, err = tx.Bucket("test")
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			t.Fatal("got nil when it should have been a bucket")
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

		// bad key
		val, err = b.Get("")
		if err != nil {
			t.Fatal(err)
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

func TestForEachWithError(t *testing.T) {
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
			err = b.ForEach(func(k string, v []byte) error {
				return fmt.Errorf("something bad happened")
			})
			if err == nil {
				return fmt.Errorf("should hve got an error")
			}
			if err.Error() != "something bad happened" {
				return fmt.Errorf("error did not match")
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}
func BenchmarkPutGet(bm *testing.B) {
	file := tempfile()
	db, err := Open(file)

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

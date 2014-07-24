// Package kvite is a simple embedded K/V store backed by SQLite
package kvite

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3" //import sqlite3 for driver
)

type (
	// DB is a wrapper around the underlying SQLite database.
	DB struct {
		db *sql.DB
	}

	// Tx wraps most interactions with the datastore.
	Tx struct {
		db      *DB
		tx      *sql.Tx
		managed bool
	}

	//Bucket represents a collection of key/value pairs inside the database.
	Bucket struct {
		name        string
		tx          *Tx
		putQuery    string
		deleteQuery string
		getQuery    string
	}
)

// Open opens a KVite datastore. The returned DB is safe for concurrent use by multiple goroutines.
// It is rarely necessary to close a DB.
func Open(filename string) (*DB, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// Close closes the database, releasing any open resources.
// It is rare to Close a DB, as the DB handle is meant to be long-lived and shared between many goroutines.
func (db *DB) Close() error {
	return db.db.Close()
}

// Begin starts a transaction.
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	t := &Tx{
		db: db,
		tx: tx,
	}
	return t, nil

}

// Transaction executes a function within the context of a  managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Rollback and Commit cannot be used inside of the function
func (db *DB) Transaction(fn func(*Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if tx.db != nil {
			tx.Rollback()
		}
	}()

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	if tx.managed {
		return errors.New("managed tx commit not allowed")
	}
	if tx.db == nil {
		// should we return an error here?
		return nil
	}

	err := tx.tx.Commit()
	tx.db = nil
	return err
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	if tx.managed {
		return errors.New("managed tx commit not allowed")
	}
	return tx.tx.Rollback()
}

// horrible hack to use a table per bucket and generate sql
func (tx *Tx) newBucket(name string) *Bucket {
	return &Bucket{
		tx:          tx,
		name:        name,
		getQuery:    fmt.Sprintf("SELECT value FROM '%s' where key = ?", name),
		deleteQuery: fmt.Sprintf("DELETE FROM '%s' where key = ?", name),
		putQuery:    fmt.Sprintf("INSERT OR REPLACE INTO '%s' (key, value) values (?,?)", name),
	}
}

// Bucket gets a bucket by name.
func (tx *Tx) Bucket(name string) (*Bucket, error) {
	var foo string

	if err := tx.tx.QueryRow("SELECT name FROM sqlite_master WHERE type=? AND name=?", "table", name).Scan(&foo); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return tx.newBucket(name), nil
}

// CreateBucket creates a new bucket and returns the new bucket.
// Returns an error if the bucket already exists.
func (tx *Tx) CreateBucket(name string) (*Bucket, error) {
	_, err := tx.tx.Exec(fmt.Sprintf("create TABLE '%s' (key text not null primary key, value blob not null)", name))

	if err != nil {
		return nil, err
	}

	return tx.newBucket(name), nil

}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns it.
// If the bucket already exists, it will return it.
func (tx *Tx) CreateBucketIfNotExists(name string) (*Bucket, error) {
	b, err := tx.Bucket(name)
	if err != nil {
		if b != nil {
			return nil, err
		}
		return tx.CreateBucket(name)
	}
	return b, nil
}

// Put sets the value for a key in the bucket. If the key exists, then its previous value will be overwritten.
func (b *Bucket) Put(key string, value []byte) error {
	_, err := b.tx.tx.Exec(b.putQuery, key, value)
	if err != nil {
		return err
	}
	return nil
}

// Delete removes a key from the bucket. If the key does not exist then nothing is done and a nil error is returned.
func (b *Bucket) Delete(key string) error {
	_, err := b.tx.tx.Exec(b.deleteQuery, key)
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the value for a key in the bucket. Returns a nil value if the key does not exist
func (b *Bucket) Get(key string) ([]byte, error) {
	var value []byte

	if err := b.tx.tx.QueryRow(b.getQuery, key).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return value, nil
}

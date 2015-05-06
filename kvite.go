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
		db           *sql.DB
		table        string
		putQuery     string
		deleteQuery  string
		getQuery     string
		foreachQuery string
		bucketsQuery string
	}

	// Tx wraps most interactions with the datastore.
	Tx struct {
		db      *DB
		tx      *sql.Tx
		managed bool
	}

	//Bucket represents a collection of key/value pairs inside the database.
	Bucket struct {
		name string
		tx   *Tx
	}
)

// Open opens a KVite datastore. The returned DB is safe for concurrent use by multiple goroutines.
// It is rarely necessary to close a DB.
func Open(filename, table string) (*DB, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}

	if table == "" {
		table = "kvite"
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	query := fmt.Sprintf("create TABLE IF NOT EXISTS '%s' (key text not null, bucket text not null, value blob not null)", table)
	if _, err := tx.Exec(query); err != nil {
		return nil, err
	}
	query = fmt.Sprintf("create UNIQUE INDEX IF NOT EXISTS '%s_kvite_key_index' ON '%s' (key, bucket)", table, table)
	if _, err := tx.Exec(query); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &DB{
		db:           db,
		table:        table,
		getQuery:     fmt.Sprintf("SELECT value FROM '%s' WHERE key = ? and bucket = ?", table),
		deleteQuery:  fmt.Sprintf("DELETE FROM '%s' WHERE key = ? AND bucket = ?", table),
		putQuery:     fmt.Sprintf("INSERT OR REPLACE INTO '%s' (key, value, bucket) VALUES (?, ?, ?)", table),
		foreachQuery: fmt.Sprintf("SELECT key, value FROM '%s' WHERE bucket = ?", table),
		bucketsQuery: fmt.Sprintf("SELECT DISTINCT bucket from '%s'", table),
	}, nil
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

// Buckets returns all the buckets
func (db *DB) Buckets() ([]string, error) {
	rows, err := db.db.Query(db.bucketsQuery)
	if err != nil {
		return nil, err
	}

	buckets := make([]string, 0, 32)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		buckets = append(buckets, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return buckets, nil
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
			_ = tx.Rollback()
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

func (tx *Tx) newBucket(name string) *Bucket {
	return &Bucket{
		tx:   tx,
		name: name,
	}
}

// Bucket gets a bucket by name.  Buckets can be created on the fly and do not "exist" until they have keys.
func (tx *Tx) Bucket(name string) (*Bucket, error) {
	return tx.newBucket(name), nil
}

// CreateBucket is provided for compatibility. It just calls Bucket.
func (tx *Tx) CreateBucket(name string) (*Bucket, error) {
	return tx.Bucket(name)

}

// CreateBucketIfNotExists is provided for compatibility. It just calls Bucket.
func (tx *Tx) CreateBucketIfNotExists(name string) (*Bucket, error) {
	return tx.Bucket(name)
}

// Put sets the value for a key in the bucket. If the key exists, then its previous value will be overwritten.
func (b *Bucket) Put(key string, value []byte) error {
	_, err := b.tx.tx.Exec(b.tx.db.putQuery, key, value, b.name)
	return err
}

// Delete removes a key from the bucket. If the key does not exist then nothing is done and a nil error is returned.
func (b *Bucket) Delete(key string) error {
	_, err := b.tx.tx.Exec(b.tx.db.deleteQuery, key, b.name)
	return err
}

// Get retrieves the value for a key in the bucket. Returns a nil value if the key does not exist
func (b *Bucket) Get(key string) ([]byte, error) {
	var value []byte

	if err := b.tx.tx.QueryRow(b.tx.db.getQuery, key, b.name).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return value, nil
}

//ForEach executes a function for each key/value pair in a bucket. If the provided function returns an error then the iteration is stopped and the error is returned to the caller.
func (b *Bucket) ForEach(fn func(k string, v []byte) error) error {
	rows, err := b.tx.tx.Query(b.tx.db.foreachQuery, b.name)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		if err := fn(key, value); err != nil {
			return err
		}
	}
	return rows.Err()
}

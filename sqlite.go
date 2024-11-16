// Package sqlite implements a connection pool for SQLite.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// TODO: add instrumentation, so we can measure how long we are waiting
// for DB connections and see the slow points. E.g. add an HTTP handler.

// DB is an SQLite connection pool.
//
// We deliberately minimize our use of database/sql machinery because
// the semantics do not match SQLite well.
//
// Instead, we choose a single connection to use for writing (because
// SQLite is single-writer) and use the rest as readers.
type DB struct {
	db      *sql.DB
	writer  chan *sql.Conn
	readers chan *sql.Conn
}

func New(dataSourceName string, readerCount int) (*DB, error) {
	if dataSourceName == ":memory:" {
		return nil, fmt.Errorf(":memory: is not supported (because multiple conns are needed); use a temp file")
	}
	// TODO: a caller could override PRAGMA query_only.
	// Consider opening two *sql.DBs, one configured as read-only,
	// to ensure read-only transactions are always such.
	db, err := sql.Open("sqlite", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("sqlite.New: %w", err)
	}
	numConns := readerCount + 1
	if err := InitDB(db, numConns); err != nil {
		return nil, fmt.Errorf("sqlite.New: %w", err)
	}

	var conns []*sql.Conn
	for i := 0; i < numConns; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlite.New: %w", err)
		}
		conns = append(conns, conn)
	}

	p := &DB{
		db:      db,
		writer:  make(chan *sql.Conn, 1),
		readers: make(chan *sql.Conn, readerCount),
	}
	p.writer <- conns[0]
	for _, conn := range conns[1:] {
		if _, err := conn.ExecContext(context.Background(), "PRAGMA query_only=1;"); err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlite.New query_only: %w", err)
		}
		p.readers <- conn
	}
	return p, nil
}

// InitDB fixes the database/sql pool to a set of fixed connections.
func InitDB(db *sql.DB, numConns int) error {
	db.SetMaxIdleConns(numConns)
	db.SetMaxOpenConns(numConns)
	db.SetConnMaxLifetime(-1)
	db.SetConnMaxIdleTime(-1)

	initQueries := []string{
		"PRAGMA journal_mode=wal;",
		"PRAGMA busy_timeout=1000",
	}

	var conns []*sql.Conn
	for i := 0; i < numConns; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			db.Close()
			return fmt.Errorf("sqlite.InitDB: %w", err)
		}
		for _, q := range initQueries {
			if _, err := conn.ExecContext(context.Background(), q); err != nil {
				db.Close()
				return fmt.Errorf("sqlite.InitDB %d: %w", i, err)
			}
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			db.Close()
			return fmt.Errorf("sqlite.InitDB: %w", err)
		}
	}
	return nil
}

func (p *DB) Close() error {
	return p.db.Close()
}

type ctxKeyType int

// CtxKey is the context value key used to store the current *Tx or *Rx.
// In general this should not be used, plumb the tx directly.
// This code is here is used for an exception: the slog package.
var CtxKey any = ctxKeyType(0)

func checkNoTx(ctx context.Context, typ string) {
	x := ctx.Value(CtxKey)
	if x == nil {
		return
	}
	orig := "unexpected"
	switch x := x.(type) {
	case *Tx:
		orig = "Tx (" + x.caller + ")"
	case *Rx:
		orig = "Rx (" + x.caller + ")"
	}
	panic(typ + " inside " + orig)
}

// Exec executes a single statement outside of a transaction.
// Useful in the rare case of PRAGMAs that cannot execute inside a tx,
// such as PRAGMA wal_checkpoint.
func (p *DB) Exec(ctx context.Context, query string, args ...interface{}) error {
	checkNoTx(ctx, "Tx")
	var conn *sql.Conn
	select {
	case <-ctx.Done():
		return fmt.Errorf("DB.Exec: %w", ctx.Err())
	case conn = <-p.writer:
	}
	var err error
	defer func() {
		p.writer <- conn
	}()
	_, err = conn.ExecContext(ctx, query, args...)
	return wrapErr("db.exec", err)
}

func (p *DB) Tx(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	checkNoTx(ctx, "Tx")
	var conn *sql.Conn
	select {
	case <-ctx.Done():
		return fmt.Errorf("Tx: %w", ctx.Err())
	case conn = <-p.writer:
	}

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		p.db.Close()
		return fmt.Errorf("Tx %w", err)
	}
	tx := &Tx{
		Rx:  &Rx{conn: conn, p: p, caller: callerOfCaller(1)},
		Now: time.Now(),
	}
	tx.ctx = context.WithValue(ctx, CtxKey, tx)

	var err error
	defer func() {
		if err == nil {
			_, err = tx.conn.ExecContext(tx.ctx, "COMMIT;")
			if err != nil {
				err = fmt.Errorf("Tx: commit: %w", err)
			}
		}
		if err != nil {
			err = p.rollback(tx.ctx, "Tx", err, tx.conn)
			// always return conn,
			// either the entire database is closed or the conn is fine.
		}
		tx.p.writer <- conn
	}()
	err = fn(tx.ctx, tx)
	return err
}

func (p *DB) Rx(ctx context.Context, fn func(ctx context.Context, rx *Rx) error) error {
	checkNoTx(ctx, "Rx")
	var conn *sql.Conn
	select {
	case <-ctx.Done():
		return ctx.Err()
	case conn = <-p.readers:
	}

	if _, err := conn.ExecContext(ctx, "BEGIN;"); err != nil {
		p.db.Close()
		return fmt.Errorf("sqlite.Rx %w", err)
	}
	rx := &Rx{conn: conn, p: p, caller: callerOfCaller(1)}
	rx.ctx = context.WithValue(ctx, CtxKey, rx)

	var err error
	defer func() {
		err = p.rollback(rx.ctx, "Rx", err, rx.conn)
		// always return conn,
		// either the entire database is closed or the conn is fine.
		rx.p.readers <- conn
	}()
	err = fn(rx.ctx, rx)
	return err
}

func (p *DB) rollback(ctx context.Context, txType string, txErr error, conn *sql.Conn) error {
	// Even if the context is cancelled,
	// we still need to rollback to finish up the transaction.
	_, err := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK;")
	if err != nil {
		// There are a few cases where an error during a transaction
		// will be reported as a rollback error:
		// 	https://sqlite.org/lang_transaction.html#response_to_errors_within_a_transaction
		// In good operation, we should never see any of these.
		// If we do however, the only one that is recoverable is SQLITE_BUSY.
		// For SQLITE_BUSY we let the DB continue, otherwise we close the DB.
		//
		// TODO: confirm this check works on all sqlite drivers.
		if !strings.Contains(err.Error(), "SQLITE_BUSY") {
			conn.Close()
			p.db.Close()
		}
		return fmt.Errorf("%s: %v: rollback failed: %w", txType, txErr, err)
	}
	return txErr
}

type Tx struct {
	*Rx
	Now time.Time
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	res, err := tx.conn.ExecContext(tx.ctx, query, args...)
	return res, wrapErr("exec", err)
}

type Rx struct {
	ctx    context.Context
	conn   *sql.Conn
	p      *DB
	caller string // for debugging
}

func (rx *Rx) Context() context.Context {
	return rx.ctx
}

func (rx *Rx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := rx.conn.QueryContext(rx.ctx, query, args...)
	return rows, wrapErr("query", err)
}

func (rx *Rx) QueryRow(query string, args ...interface{}) *Row {
	rows, err := rx.conn.QueryContext(rx.ctx, query, args...)
	return &Row{err: err, rows: rows}
}

// Row is equivalent to *sql.Row, but we provide a more useful error.
type Row struct {
	err  error
	rows *sql.Rows
}

func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return wrapErr("QueryRow", r.err)
	}

	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return wrapErr("QueryRow.Scan", err)
		}
		return wrapErr("QueryRow.Scan", sql.ErrNoRows)
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return wrapErr("QueryRow.Scan", err)
	}
	return wrapErr("QueryRow.Scan", r.rows.Close())
}

func wrapErr(prefix string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %s: %w", callerOfCaller(2), prefix, err)
}

func callerOfCaller(depth int) string {
	caller := "sqlite.unknown"
	pc := make([]uintptr, 3)
	const addedSkip = 3 // runtime.Callers, callerOfCaller, our caller (e.g. wrapErr or Rx)
	if n := runtime.Callers(addedSkip+depth-1, pc[:]); n > 0 {
		frames := runtime.CallersFrames(pc[:n])
		frame, _ := frames.Next()
		if frame.Function != "" {
			caller = frame.Function
		}
		// This is a special case.
		//
		// We expect people to wrap the sqlite.Tx/Rx objects
		// in another domain-specific Tx/Rx object. That means
		// they almost certainly have matching Tx/Rx methods,
		// which aren't useful for debugging. So if we see that,
		// we remove it.
		if strings.HasSuffix(caller, ".Tx") || strings.HasSuffix(caller, ".Rx") {
			frame, more := frames.Next()
			if more && frame.Function != "" {
				caller = frame.Function
			}
		}
	}
	if i := strings.LastIndexByte(caller, '/'); i >= 0 {
		caller = caller[i+1:]
	}
	return caller
}

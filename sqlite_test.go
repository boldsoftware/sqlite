package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestWrapErr(t *testing.T) {
	err := wrapErr("prefix", nil)
	if err != nil {
		t.Fatalf("err=%v, want nil", err)
	}

	func() {
		// wrapErr skips the function calling it,
		// so the anonymous function is skipped over.
		err = wrapErr("prefix", errors.New("testerr"))
		if err == nil {
			t.Fatal("err=nil, want an error")
		}
	}()
	got := err.Error()
	const want = "sqlite.TestWrapErr: prefix: testerr"
	if got != want {
		t.Errorf("err=%q, want %q", got, want)
	}
}

func TestPool(t *testing.T) {
	p, err := New(filepath.Join(t.TempDir(), "testpool.sqlite"), 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
		if _, err := tx.Exec("CREATE TABLE t (c);"); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT INTO t (c) VALUES (1);")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count, count2 int
	err = p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
		// Use a background context here directly to work around any
		// context checking we do to stop nested transactions.
		// We just want to demonstrate two read transactions can be open
		// simultaneously.
		err = p.Rx(context.Background(), func(ctx context.Context, rx *Rx) error {
			return rx.QueryRow("SELECT count(*) FROM t;").Scan(&count2)
		})
		if err != nil {
			return fmt.Errorf("rx2: %w", err)
		}
		return rx.QueryRow("SELECT count(*) FROM t;").Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}
	if count2 != 1 {
		t.Fatalf("count2=%d, want 1", count)
	}

	err = p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("INSERT INTO t (c) VALUES (1);")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	wantErr := fmt.Errorf("we want this error")
	err = p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("INSERT INTO t (c) VALUES (1);")
		if err != nil {
			return err
		}
		return wantErr // rollback
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want wantErr", err)
	}

	err = p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
		return rx.QueryRow("SELECT count(*) FROM t;").Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNestPanic(t *testing.T) {
	p, err := New(filepath.Join(t.TempDir(), "testpool.sqlite"), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx := context.Background()

	t.Run("rx-inside-rx", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expecting nested Rx panic, got none")
			}
			if want := "Rx inside Rx (sqlite.TestNestPanic.func1)"; r != want {
				t.Fatalf("panic=%q, want %q", r, want)
			}
		}()
		err := p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
			return p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("rx-inside-tx", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expecting nested Rx panic, got none")
			}
			if want := "Rx inside Tx (sqlite.TestNestPanic.func2)"; r != want {
				t.Fatalf("panic=%q, want %q", r, want)
			}
		}()
		err := p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
			return p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tx-inside-tx", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expecting nested Rx panic, got none")
			}
			if want := "Tx inside Tx (sqlite.TestNestPanic.func3)"; r != want {
				t.Fatalf("panic=%q, want %q", r, want)
			}
		}()
		err := p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
			return p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tx-inside-rx", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expecting nested Rx panic, got none")
			}
			if want := "Tx inside Rx (sqlite.TestNestPanic.func4)"; r != want {
				t.Fatalf("panic=%q, want %q", r, want)
			}
		}()
		err := p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
			return p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestExpiredContextRollback(t *testing.T) {
	// Set up the DB.
	p, err := New(filepath.Join(t.TempDir(), "testpool.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	bg := context.Background()
	err = p.Tx(bg, func(ctx context.Context, tx *Tx) error {
		_, err := tx.Exec("CREATE TABLE t (c);")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Cancel a context mid-way through an Rx.
	ctx, cancel := context.WithCancel(bg)
	err = p.Rx(ctx, func(ctx context.Context, rx *Rx) error {
		cancel()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that a subsequent transaction succeeds.
	var count int
	err = p.Rx(bg, func(ctx context.Context, rx *Rx) error {
		return rx.QueryRow("SELECT count(*) FROM t;").Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0", count)
	}

	// Do the same for a Tx.
	ctx, cancel = context.WithCancel(bg)
	fmt.Println("cancel")
	err = p.Tx(ctx, func(ctx context.Context, tx *Tx) error {
		cancel()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that a subsequent transaction succeeds.
	err = p.Tx(bg, func(ctx context.Context, tx *Tx) error {
		return tx.QueryRow("SELECT count(*) FROM t;").Scan(&count)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0", count)
	}
}

func TestExecWithoutTx(t *testing.T) {
	p, err := New(filepath.Join(t.TempDir(), "testexecwithouttx.sqlite"), 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := p.Exec(ctx, "PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

package sync2

import "testing"

func TestConsolidator(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"

	orig, added := con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}
	if got, want := orig.pending, 1; got != want {
		t.Fatalf("wrong pending count: got = %v, want = %v", got, want)
	}

	dup, added := con.Create(sql)
	if added {
		t.Fatalf("did not expect consolidator to register a new entry")
	}
	if got, want := dup.pending, 2; got != want {
		t.Fatalf("wrong pending count: got = %v, want = %v", got, want)
	}

	// Limit version checks for the limit.
	_, _, limited := con.CreateWithLimit(sql, 2)
	if !limited {
		t.Fatal("CreateWithLimit() should not have gone over the limit")
	}

	result := 1
	go func() {
		orig.Result = &result
		orig.Broadcast()
	}()
	dup.Wait()

	if *orig.Result.(*int) != result {
		t.Errorf("failed to pass result")
	}
	if *orig.Result.(*int) != *dup.Result.(*int) {
		t.Fatalf("failed to share the result")
	}

	// Running the query again should add a new entry since the original
	// query execution completed
	orig, added = con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}
	// pending starts at 1 again because it's a new "round".
	if got, want := orig.pending, 1; got != want {
		t.Fatalf("wrong pending count: got = %v, want = %v", got, want)
	}
}

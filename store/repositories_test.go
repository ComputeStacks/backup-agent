package store

import "testing"

// TestDeleteRepository covers m7: Trash must be able to drop the repository
// projection row so the controller's backups list doesn't keep a dead repo.
func TestDeleteRepository(t *testing.T) {
	s := open(t, Options{})
	if err := s.UpsertRepository(ctx, Repository{Name: "vol-1", Archives: []string{"a1"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRepository(ctx, "vol-1"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.GetRepository(ctx, "vol-1"); found {
		t.Fatal("repository still present after delete")
	}
	var et, id, op string
	if err := s.control.QueryRowContext(ctx,
		`SELECT entity_type, entity_id, op FROM changelog ORDER BY seq DESC LIMIT 1`).Scan(&et, &id, &op); err != nil {
		t.Fatal(err)
	}
	if et != "repository" || id != "vol-1" || op != "delete" {
		t.Fatalf("changelog = %s %s %s, want repository vol-1 delete", et, id, op)
	}
	// Deleting an absent repository is a silent no-op.
	if err := s.DeleteRepository(ctx, "nope"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}

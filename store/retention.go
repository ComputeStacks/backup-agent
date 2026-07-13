package store

import (
	"context"
	"fmt"
	"math"
)

// PruneChangelog deletes changelog rows that are safe to drop, and returns the
// number deleted. Two independent rules (OR'd), both bounded by an age floor so a
// premature/erroneous ack can't drop rows the controller still needs:
//
//   - ack-gated: seq <= the controller's acked watermark AND older than
//     minAgeSec. This is the normal path once the controller is acking.
//   - age fallback: older than maxAgeSec regardless of ack. This bounds growth
//     when no controller is acking yet — e.g. between the agent deploy and the
//     controller integrating (acked stays 0). Pass maxAgeSec <= 0 to disable it.
//
// AUTOINCREMENT keeps seq climbing across deletes, so a pruned range never
// reappears and ChangelogSince's "seq > cursor" tolerates the gaps by design.
func (s *Store) PruneChangelog(ctx context.Context, now, minAgeSec, maxAgeSec int64) (int64, error) {
	acked, err := s.GetChangelogAcked(ctx)
	if err != nil {
		return 0, err
	}
	minCutoff := now - minAgeSec
	maxCutoff := int64(math.MinInt64) // disabled: no created_at can be below this
	if maxAgeSec > 0 {
		maxCutoff = now - maxAgeSec
	}
	res, err := s.control.ExecContext(ctx, `
		DELETE FROM changelog
		WHERE (seq <= ? AND created_at < ?)
		   OR (created_at < ?)
	`, acked, minCutoff, maxCutoff)
	if err != nil {
		return 0, fmt.Errorf("store: prune changelog: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: prune changelog rows affected: %w", err)
	}
	return n, nil
}

// DeleteTerminalTasksBefore reaps terminal tasks (completed/failed/cancelled) last
// updated before `before`, returning the number deleted. This bounds the tasks
// table under continuous scheduled backups — every kind is reaped, not just
// exports (only exports carried an expiry). `before` must be older than the
// longest export presigned-URL TTL so a completed export whose download link is
// still live is not reaped early. It is local housekeeping of the node's working
// set, NOT node truth, so it is never changelogged: the controller keeps and
// prunes its own task projection independently.
func (s *Store) DeleteTerminalTasksBefore(ctx context.Context, before int64) (int64, error) {
	res, err := s.control.ExecContext(ctx, `
		DELETE FROM tasks
		WHERE status IN (?, ?, ?) AND updated_at < ?
	`, TaskCompleted, TaskFailed, TaskCancelled, before)
	if err != nil {
		return 0, fmt.Errorf("store: prune terminal tasks: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: prune terminal tasks rows affected: %w", err)
	}
	return n, nil
}

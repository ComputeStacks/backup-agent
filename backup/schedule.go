/*
Durable backup scheduler.

Replaces the old in-RAM robfig runner + Consul schedule mirror. robfig/cron is
kept only as the cron-string PARSER (ParseStandard + Schedule.Next), so the cron
syntax the controller emits is unchanged. Per-volume backup schedules live in
control.db (the `schedules` table): a tick loop fires due schedules by inserting a
volume.backup task and advancing next_fire_at in one transaction (durable
exactly-once). Node maintenance (prune/compact/changelog-prune/task-retention)
runs on the same tick with skip-on-misfire.
*/
package backup

import (
	"context"
	"cs-agent/store"
	"cs-agent/types"
	"os"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/spf13/viper"
)

const schedulerTick = 20 * time.Second

// Scheduler owns per-volume backup scheduling + node maintenance, driven off
// control.db. Construct with NewScheduler and run with Run (until ctx is done).
type Scheduler struct {
	st          *store.Store
	hostname    string
	dispatch    func() // poke the dispatcher after enqueuing work (non-blocking)
	tick        time.Duration
	reconcileCh chan struct{}
	maint       []*maintJob
}

// maintJob is a node-wide maintenance task on a cron schedule with skip-on-misfire
// (next-fire held in RAM; a node down over a slot simply skips it).
type maintJob struct {
	name string
	expr string
	next time.Time
	run  func(ctx context.Context)
}

// NewScheduler builds the scheduler. dispatch is called (non-blocking) after a
// backup/trash task is enqueued so the dispatcher wakes promptly; it may be nil.
func NewScheduler(st *store.Store, dispatch func()) *Scheduler {
	hostname, _ := os.Hostname()
	s := &Scheduler{
		st:          st,
		hostname:    hostname,
		dispatch:    dispatch,
		tick:        schedulerTick,
		reconcileCh: make(chan struct{}, 1),
	}
	s.maint = []*maintJob{
		{name: "prune", expr: viper.GetString("backups.prune_freq"), run: func(ctx context.Context) { prune(ctx, st) }},
		{name: "compact", expr: viper.GetString("backups.compact_freq"), run: func(ctx context.Context) { compact(ctx, st) }},
		{name: "housekeeping", expr: viper.GetString("changelog.prune_freq"), run: s.housekeeping},
	}
	return s
}

// ReconcileSignal asks the scheduler to reconcile volume schedules on its next
// loop iteration. Non-blocking + coalescing: a late DOWN handler never blocks.
func (s *Scheduler) ReconcileSignal() {
	select {
	case s.reconcileCh <- struct{}{}:
	default:
	}
}

// Run drives the scheduler until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	backupLogger().Info("Starting backup scheduler")
	now := time.Now()
	for _, m := range s.maint {
		if m.expr != "" {
			m.next = nextFire(m.expr, now)
		}
	}
	s.reconcile(ctx) // boot rebuild

	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			backupLogger().Info("Backup scheduler stopping")
			return
		case <-s.reconcileCh:
			s.reconcile(ctx)
		case <-ticker.C:
			s.fireDue(ctx)
			s.runMaintenance(ctx)
			s.reconcile(ctx) // periodic backstop
		}
	}
}

// fireDue enqueues a volume.backup task for every schedule whose next_fire_at has
// passed, advancing next_fire_at in the same transaction (exactly-once).
func (s *Scheduler) fireDue(ctx context.Context) {
	defer sentry.Recover()
	now := time.Now()
	due, err := s.st.ListDueSchedules(ctx, now.Unix())
	if err != nil {
		backupLogger().Warn("Scheduler: list due schedules", "error", err.Error())
		return
	}
	fired := false
	for _, sc := range due {
		next := nextFire(sc.CronExpr, now)
		if next.IsZero() {
			// Unparseable/never-firing cron slipped in — drop the schedule so it
			// doesn't re-evaluate as due every tick.
			backupLogger().Warn("Scheduler: dropping schedule with unparseable cron", "volume", sc.VolumeName, "cron", sc.CronExpr)
			_ = s.st.DeleteSchedule(ctx, sc.VolumeName)
			continue
		}
		// Re-check the volume still wants backups (config may have changed since
		// the row was written) and grab its project id for the task/changelog.
		v, found, err := s.st.GetVolume(ctx, sc.VolumeName)
		if err != nil {
			backupLogger().Warn("Scheduler: load volume for due schedule", "volume", sc.VolumeName, "error", err.Error())
			continue
		}
		if !found {
			_ = s.st.DeleteSchedule(ctx, sc.VolumeName)
			continue
		}
		vol, err := types.LoadVolume(v.Config)
		if err != nil || !vol.Backup || vol.Trash {
			_ = s.st.DeleteSchedule(ctx, sc.VolumeName)
			continue
		}
		task := store.Task{
			ID:        uuid.New().String(),
			Name:      "volume.backup",
			Node:      s.hostname,
			Volume:    sc.VolumeName,
			ProjectID: strconv.Itoa(vol.ProjectID),
			Archive:   "auto",
		}
		if _, err := s.st.FireDueBackup(ctx, task, next.Unix()); err != nil {
			backupLogger().Warn("Scheduler: fire due backup", "volume", sc.VolumeName, "error", err.Error())
			continue
		}
		fired = true
	}
	if fired && s.dispatch != nil {
		s.dispatch()
	}
}

// runMaintenance runs any node maintenance job whose cron slot has passed
// (skip-on-misfire: next is always recomputed from now, never replayed).
func (s *Scheduler) runMaintenance(ctx context.Context) {
	now := time.Now()
	for _, m := range s.maint {
		if m.expr == "" {
			continue
		}
		if m.next.IsZero() {
			m.next = nextFire(m.expr, now)
			continue
		}
		if !now.Before(m.next) {
			m.run(ctx)
			m.next = nextFire(m.expr, now)
		}
	}
}

// housekeeping prunes acked/aged changelog rows and reaps terminal task rows.
func (s *Scheduler) housekeeping(ctx context.Context) {
	defer sentry.Recover()
	now := time.Now().Unix()
	minAge := int64(viper.GetInt("changelog.prune_min_age_sec"))
	maxAge := int64(viper.GetInt("changelog.prune_max_age_sec"))
	if n, err := s.st.PruneChangelog(ctx, now, minAge, maxAge); err != nil {
		backupLogger().Warn("Scheduler: changelog prune", "error", err.Error())
	} else if n > 0 {
		backupLogger().Info("Pruned changelog rows", "count", n)
	}
	if retention := int64(viper.GetInt("tasks.retention_sec")); retention > 0 {
		if n, err := s.st.DeleteTerminalTasksBefore(ctx, now-retention); err != nil {
			backupLogger().Warn("Scheduler: task retention", "error", err.Error())
		} else if n > 0 {
			backupLogger().Info("Reaped terminal tasks", "count", n)
		}
	}
}

// reconcile brings the schedules table in line with volume desired-state. Gated by
// the volumes-populated sentinel so an unpopulated control.db never wipes every
// schedule. A trashed volume gets a (stable-id, idempotent) volume.trash task and
// its schedule removed; a backup-disabled or vanished volume just loses its
// schedule; a new/changed cron gets Next(now) (no catch-up on first schedule, so a
// fleet backfill can't trigger a backup storm).
func (s *Scheduler) reconcile(ctx context.Context) {
	defer sentry.Recover()
	populated, err := s.st.IsPopulated(ctx, store.MetaVolumesPopulated)
	if err != nil {
		backupLogger().Warn("Scheduler: check volumes populated", "error", err.Error())
		return
	}
	if !populated {
		backupLogger().Debug("Scheduler: volumes not populated, skipping reconcile")
		return
	}

	vols, err := s.st.ListVolumesByNode(ctx, s.hostname)
	if err != nil {
		backupLogger().Warn("Scheduler: list volumes", "error", err.Error())
		return
	}

	now := time.Now()
	seen := map[string]bool{}
	trashed := false
	for _, sv := range vols {
		vol, err := types.LoadVolume(sv.Config)
		if err != nil {
			backupLogger().Warn("Scheduler: parse volume", "volume", sv.Name, "error", err.Error())
			continue
		}
		if vol.Node != s.hostname {
			continue
		}
		seen[vol.Name] = true

		if vol.Trash {
			// Enqueue teardown once (stable id => idempotent) and stop scheduling.
			if _, err := s.st.CreateTask(ctx, store.Task{
				ID:        "volume.trash:" + vol.Name,
				Name:      "volume.trash",
				Node:      s.hostname,
				Volume:    vol.Name,
				ProjectID: strconv.Itoa(vol.ProjectID),
			}); err != nil {
				backupLogger().Warn("Scheduler: enqueue trash", "volume", vol.Name, "error", err.Error())
			} else {
				trashed = true
			}
			_ = s.st.DeleteSchedule(ctx, vol.Name)
			continue
		}

		if !vol.Backup || vol.Freq == "" {
			_ = s.st.DeleteSchedule(ctx, vol.Name)
			continue
		}

		existing, found, _ := s.st.GetSchedule(ctx, vol.Name)
		if !found || existing.CronExpr != vol.Freq {
			next := nextFire(vol.Freq, now)
			if next.IsZero() {
				backupLogger().Warn("Scheduler: invalid cron for volume", "volume", vol.Name, "cron", vol.Freq)
				continue
			}
			if err := s.st.PutSchedule(ctx, vol.Name, vol.Freq, next.Unix()); err != nil {
				backupLogger().Warn("Scheduler: put schedule", "volume", vol.Name, "error", err.Error())
			}
		}
		// Unchanged cron: leave next_fire_at untouched (never push it forward).
	}

	// Drop schedules for volumes no longer present/owned here.
	all, err := s.st.ListSchedules(ctx)
	if err == nil {
		for _, sc := range all {
			if !seen[sc.VolumeName] {
				_ = s.st.DeleteSchedule(ctx, sc.VolumeName)
			}
		}
	}

	if trashed && s.dispatch != nil {
		s.dispatch()
	}
}

// nextFire parses a standard 5-field cron expression (robfig, parser only) and
// returns the next fire time after `from`; a zero time signals an unparseable or
// never-firing expression.
func nextFire(expr string, from time.Time) time.Time {
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}
	}
	return sched.Next(from)
}

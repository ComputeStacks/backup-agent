package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"

	"cs-agent/store"
)

func TestAdminTaskCreate(t *testing.T) {
	e := newTestEnv(t)
	body := []byte(`{"id":"jid-1","project_id":"proj-a","name":"volume.backup","node":"node-a","volume":"vol-1"}`)

	resp := e.do("POST", "/v1/admin/tasks", e.adminTok, body)
	mustStatus(t, resp, http.StatusAccepted)
	var cr taskCreateResponse
	if err := json.Unmarshal(readBody(t, resp), &cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cr.ID != "jid-1" || !cr.Created {
		t.Fatalf("create response: %+v", cr)
	}

	// The task shows up on the controller pull channel as a "task" upsert.
	resp = e.do("GET", "/v1/admin/changelog?since=0&entity_type=task", e.adminTok, nil)
	mustStatus(t, resp, http.StatusOK)
	var list changelogListResponse
	if err := json.Unmarshal(readBody(t, resp), &list); err != nil {
		t.Fatalf("decode changelog: %v", err)
	}
	if len(list.Entries) != 1 || list.Entries[0].EntityID != "jid-1" || list.Entries[0].EntityType != "task" {
		t.Fatalf("entries: %+v", list.Entries)
	}
	var snap store.Task
	if err := json.Unmarshal(list.Entries[0].Payload, &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.Name != "volume.backup" || snap.Node != "node-a" {
		t.Fatalf("snapshot: %+v", snap)
	}

	// A duplicate POST (same id) is accepted but created=false — no re-dispatch.
	resp = e.do("POST", "/v1/admin/tasks", e.adminTok, body)
	mustStatus(t, resp, http.StatusAccepted)
	if err := json.Unmarshal(readBody(t, resp), &cr); err != nil {
		t.Fatalf("decode dup: %v", err)
	}
	if cr.Created {
		t.Fatal("duplicate create returned created=true, want false")
	}
}

func TestAdminTaskCreate_Validation(t *testing.T) {
	e := newTestEnv(t)
	cases := []struct {
		name string
		body string
		want int
	}{
		{"bad json", `{`, http.StatusBadRequest},
		{"missing id", `{"name":"volume.backup","node":"n"}`, http.StatusBadRequest},
		{"missing name", `{"id":"x","node":"n"}`, http.StatusBadRequest},
		{"missing node", `{"id":"x","name":"volume.backup"}`, http.StatusBadRequest},
		{"ok", `{"id":"okid","name":"volume.backup","node":"n"}`, http.StatusAccepted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := e.do("POST", "/v1/admin/tasks", e.adminTok, []byte(c.body))
			mustStatus(t, resp, c.want)
		})
	}
}

func TestAdminTaskCancel(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.st.CreateTask(ctxBG, store.Task{ID: "c1", Name: "volume.backup", Node: "n"}); err != nil {
		t.Fatalf("seed task: %v", err)
	}
	resp := e.do("DELETE", "/v1/admin/tasks/c1", e.adminTok, nil)
	mustStatus(t, resp, http.StatusOK)
	var cr taskCancelResponse
	if err := json.Unmarshal(readBody(t, resp), &cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cr.ID != "c1" || !cr.Cancelled {
		t.Fatalf("cancel response: %+v", cr)
	}
}

func TestAdminFirewallPutDelete(t *testing.T) {
	e := newTestEnv(t)

	resp := e.do("PUT", "/v1/admin/nodes/node-a/firewall_rules", e.adminTok, []byte(`{"rules":[{"proto":"tcp","port":80}]}`))
	mustStatus(t, resp, http.StatusOK)

	resp = e.do("GET", "/v1/admin/changelog?since=0&entity_type=firewall_rule", e.adminTok, nil)
	var list changelogListResponse
	if err := json.Unmarshal(readBody(t, resp), &list); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(list.Entries) != 1 || list.Entries[0].EntityID != "node-a" || list.Entries[0].EntityType != "firewall_rule" {
		t.Fatalf("entries: %+v", list.Entries)
	}

	// Invalid JSON body rejected.
	resp = e.do("PUT", "/v1/admin/nodes/node-a/firewall_rules", e.adminTok, []byte(`{`))
	mustStatus(t, resp, http.StatusBadRequest)

	resp = e.do("DELETE", "/v1/admin/nodes/node-a/firewall_rules", e.adminTok, nil)
	mustStatus(t, resp, http.StatusOK)
}

func TestAdminVolumePutDelete(t *testing.T) {
	e := newTestEnv(t)
	body := []byte(`{"name":"vol-1","node":"node-a","freq":"0 2 * * *","backup":true}`)

	resp := e.do("PUT", "/v1/admin/projects/proj-a/volumes/vol-1", e.adminTok, body)
	mustStatus(t, resp, http.StatusOK)

	resp = e.do("GET", "/v1/admin/changelog?since=0&entity_type=volume", e.adminTok, nil)
	var list changelogListResponse
	if err := json.Unmarshal(readBody(t, resp), &list); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(list.Entries) != 1 || list.Entries[0].EntityID != "vol-1" || list.Entries[0].ProjectID != "proj-a" {
		t.Fatalf("entries: %+v", list.Entries)
	}

	// A body without node is rejected (the store needs it as an index column).
	resp = e.do("PUT", "/v1/admin/projects/proj-a/volumes/vol-2", e.adminTok, []byte(`{"name":"vol-2"}`))
	mustStatus(t, resp, http.StatusBadRequest)

	resp = e.do("DELETE", "/v1/admin/projects/proj-a/volumes/vol-1", e.adminTok, nil)
	mustStatus(t, resp, http.StatusOK)
}

// TestAdminGroup2Routes_RequireAdmin proves the DOWN endpoints are admin-scoped:
// a valid tenant Bearer (wrong scope) is rejected 403.
func TestAdminGroup2Routes_RequireAdmin(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	for _, tc := range []struct {
		method, path string
		body         []byte
	}{
		{"POST", "/v1/admin/tasks", []byte(`{"id":"x","name":"volume.backup","node":"n"}`)},
		{"DELETE", "/v1/admin/tasks/x", nil},
		{"PUT", "/v1/admin/nodes/node-a/firewall_rules", []byte(`{"rules":[]}`)},
		{"DELETE", "/v1/admin/nodes/node-a/firewall_rules", nil},
		{"PUT", "/v1/admin/projects/proj-a/volumes/vol-1", []byte(`{"node":"node-a"}`)},
		{"DELETE", "/v1/admin/projects/proj-a/volumes/vol-1", nil},
	} {
		resp := e.do(tc.method, tc.path, "tok-a", tc.body)
		mustStatus(t, resp, http.StatusForbidden)
	}
}

package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cs-agent/store"
)

// ctxBG is the context for direct store calls in tests.
var ctxBG = context.Background()

// hashToken mirrors the agent's auth derivation: sha256(token) → hex.
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// testEnv is a server backed by a real store in a temp dir, plus an httptest
// server. The store is the actual store.Store — no mocks — so the
// tenant-isolation guarantees are exercised end to end.
type testEnv struct {
	t        *testing.T
	dataDir  string
	st       *store.Store
	srv      *Server
	httpsrv  *httptest.Server
	adminTok string
}

const adminToken = "admin-secret-token-per-node"

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv := New(Config{
		ListenAddr:     "127.0.0.1:0",
		AdminTokenHash: hashToken(adminToken),
		MaxBodyBytes:   1 << 20, // 1 MiB body cap for the 413 test
		ProxyToConsul:  false,   // proxy MUST stay off for these tests
	}, st, nil)

	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)

	return &testEnv{
		t:        t,
		dataDir:  dir,
		st:       st,
		srv:      srv,
		httpsrv:  hs,
		adminTok: adminToken,
	}
}

// provisionTenant creates a tenant+project DB directly via the store (the admin
// route is tested separately).
func (e *testEnv) provisionTenant(projectID, token, status string) {
	e.t.Helper()
	if err := e.st.UpsertTenant(ctxBG, projectID, hashToken(token), status); err != nil {
		e.t.Fatalf("UpsertTenant(%s): %v", projectID, err)
	}
	if err := e.st.CreateProjectDB(ctxBG, projectID); err != nil {
		e.t.Fatalf("CreateProjectDB(%s): %v", projectID, err)
	}
}

// do issues a request to the test server. token "" omits the Authorization
// header; otherwise it is sent as "Bearer <token>".
func (e *testEnv) do(method, path, token string, body []byte) *http.Response {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, e.httpsrv.URL+path, r)
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.httpsrv.Client().Do(req)
	if err != nil {
		e.t.Fatalf("do %s %s: %v", method, path, err)
	}
	return resp
}

// doRaw issues a request with a verbatim Authorization header (for the malformed
// header cases).
func (e *testEnv) doRaw(method, path, authHeader string, body []byte) *http.Response {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, e.httpsrv.URL+path, r)
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := e.httpsrv.Client().Do(req)
	if err != nil {
		e.t.Fatalf("do %s %s: %v", method, path, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		b := readBody(t, resp)
		t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, want, b)
	}
}

// --- Auth ---------------------------------------------------------------------

func TestAuth_NoOrGarbageToken(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	cases := []struct {
		name string
		auth string // verbatim Authorization header; "" = none
	}{
		{"no header", ""},
		{"empty bearer", "Bearer "},
		{"wrong scheme", "Basic abc123"},
		{"garbage token", "Bearer not-a-real-token"},
		{"no bearer prefix", "tok-a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := e.doRaw("GET", "/v1/db/anything", tc.auth, nil)
			mustStatus(t, resp, http.StatusUnauthorized)
		})
	}
}

func TestAuth_ValidTenant_CustomerScope(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	// PUT then GET round-trips within the customer's own scope.
	resp := e.do("PUT", "/v1/db/wordpress/plugins", "tok-a", []byte(`{"x":1}`))
	mustStatus(t, resp, http.StatusOK)
	resp = e.do("GET", "/v1/db/wordpress/plugins", "tok-a", nil)
	mustStatus(t, resp, http.StatusOK)
	if got := string(readBody(t, resp)); got != `{"x":1}` {
		t.Fatalf("round-trip value = %q", got)
	}
}

func TestAuth_SuspendedTenant_403(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-sus", "tok-sus", "suspended")

	resp := e.do("GET", "/v1/db/whatever", "tok-sus", nil)
	mustStatus(t, resp, http.StatusForbidden)
}

func TestAuth_TrashedTenant_403(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-tr", "tok-tr", "trashed")

	resp := e.do("PUT", "/v1/db/x", "tok-tr", []byte(`v`))
	mustStatus(t, resp, http.StatusForbidden)
}

func TestAuth_AdminToken_AdminScope(t *testing.T) {
	e := newTestEnv(t)
	// Admin can hit an admin route even with no tenants provisioned.
	resp := e.do("PUT", "/v1/admin/projects/proj-x/managed/metadata", e.adminTok, []byte(`{"ok":true}`))
	mustStatus(t, resp, http.StatusOK)
}

// TestAuth_AdminPrecedence: a token whose sha256 matches the admin hash is ADMIN
// even if a tenant row also maps that same hash. Admin is checked first.
func TestAuth_AdminPrecedence(t *testing.T) {
	e := newTestEnv(t)
	// Provision a tenant whose token IS the admin token (same hash). Admin must win.
	e.provisionTenant("proj-collide", adminToken, "active")

	// As admin it can reach an admin-only route (a customer token would 403).
	resp := e.do("PUT", "/v1/admin/projects/proj-other/managed/metadata", e.adminTok, []byte(`{}`))
	mustStatus(t, resp, http.StatusOK)

	// And on a customer route the admin token is rejected (it is admin scope, not
	// customer scope) — it does NOT get treated as proj-collide's customer.
	resp = e.do("GET", "/v1/db/secret", e.adminTok, nil)
	mustStatus(t, resp, http.StatusForbidden)
}

// TestAuth_CustomerOnAdminRoute_403: a customer token hitting /v1/admin/* is 403.
func TestAuth_CustomerOnAdminRoute_403(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	routes := []struct {
		method, path string
		body         []byte
	}{
		{"PUT", "/v1/admin/projects/proj-b/managed/metadata", []byte(`{}`)},
		{"GET", "/v1/admin/projects/proj-b/db/x", nil},
		{"PUT", "/v1/admin/projects/proj-b/db/x", []byte(`v`)},
		{"DELETE", "/v1/admin/projects/proj-b/db/x", nil},
		{"PUT", "/v1/admin/tenants/proj-b", []byte(`{"token_hash":"deadbeef"}`)},
		{"DELETE", "/v1/admin/tenants/proj-b", nil},
	}
	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			resp := e.do(rt.method, rt.path, "tok-a", rt.body)
			mustStatus(t, resp, http.StatusForbidden)
		})
	}
}

// TestAuth_NoAdminHash_DisablesAdmin: with an empty admin hash, even the right
// token can't get admin scope (it resolves to an unknown tenant → 401).
func TestAuth_NoAdminHash_DisablesAdmin(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(Config{AdminTokenHash: ""}, st, nil)
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)

	req, _ := http.NewRequest("PUT", hs.URL+"/v1/admin/projects/p/managed/metadata", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	resp, err := hs.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	// Empty admin hash → admin branch skipped → unknown tenant → 401.
	mustStatus(t, resp, http.StatusUnauthorized)
}

// --- Customer /v1/db ----------------------------------------------------------

func TestCustomerDB_RoundTripLargeValue(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	// >512 KB value — the Consul ceiling we are killing. Must store + read back.
	big := bytes.Repeat([]byte("A"), 600*1024)
	resp := e.do("PUT", "/v1/db/big", "tok-a", big)
	mustStatus(t, resp, http.StatusOK)

	resp = e.do("GET", "/v1/db/big", "tok-a", nil)
	mustStatus(t, resp, http.StatusOK)
	got := readBody(t, resp)
	if !bytes.Equal(got, big) {
		t.Fatalf("large value round-trip: got %d bytes, want %d", len(got), len(big))
	}
}

func TestCustomerDB_Delete(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	mustStatus(t, e.do("PUT", "/v1/db/k", "tok-a", []byte("v")), http.StatusOK)
	mustStatus(t, e.do("GET", "/v1/db/k", "tok-a", nil), http.StatusOK)
	mustStatus(t, e.do("DELETE", "/v1/db/k", "tok-a", nil), http.StatusOK)
	mustStatus(t, e.do("GET", "/v1/db/k", "tok-a", nil), http.StatusNotFound)
}

// TestCustomerDB_CrossTenantIsolation is the core tenant-isolation guarantee:
// tenant A's token can never read tenant B's data, and the project is derived
// from the token (not the path).
func TestCustomerDB_CrossTenantIsolation(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")
	e.provisionTenant("proj-b", "tok-b", "active")

	// B writes a secret to its own /v1/db/secret.
	mustStatus(t, e.do("PUT", "/v1/db/secret", "tok-b", []byte("B-only")), http.StatusOK)

	// A reads the SAME path with its own token — A has nothing there; it gets a
	// 404, NOT B's value. There is no path component A could set to select B.
	resp := e.do("GET", "/v1/db/secret", "tok-a", nil)
	mustStatus(t, resp, http.StatusNotFound)

	// A writes its own value at the same path; the two are independent.
	mustStatus(t, e.do("PUT", "/v1/db/secret", "tok-a", []byte("A-only")), http.StatusOK)
	if got := string(readBody(t, e.do("GET", "/v1/db/secret", "tok-a", nil))); got != "A-only" {
		t.Fatalf("A reads %q, want A-only", got)
	}
	if got := string(readBody(t, e.do("GET", "/v1/db/secret", "tok-b", nil))); got != "B-only" {
		t.Fatalf("B reads %q, want B-only", got)
	}
}

// TestCustomerManaged_ReadOnly: a customer can GET /v1/managed but there is NO
// customer-facing managed WRITE route (PUT/DELETE on /v1/managed 405/404).
func TestCustomerManaged_ReadOnly(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	// Platform seeds managed_kv directly (as the controller would, via admin).
	if err := e.st.ManagedPut(ctxBG, "proj-a", "info", "application/json", []byte(`{"m":1}`)); err != nil {
		t.Fatalf("ManagedPut: %v", err)
	}
	resp := e.do("GET", "/v1/managed/info", "tok-a", nil)
	mustStatus(t, resp, http.StatusOK)
	if got := string(readBody(t, resp)); got != `{"m":1}` {
		t.Fatalf("managed read = %q", got)
	}

	// There is no customer managed-write route. PUT /v1/managed/* is not
	// registered → ServeMux returns 405 (method not allowed for the path) or 404.
	resp = e.do("PUT", "/v1/managed/info", "tok-a", []byte("nope"))
	if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("customer managed PUT status = %d, want 405 or 404", resp.StatusCode)
	}
	// And the value is unchanged.
	if got := string(readBody(t, e.do("GET", "/v1/managed/info", "tok-a", nil))); got != `{"m":1}` {
		t.Fatalf("managed value changed via customer PUT: %q", got)
	}
}

// --- Admin --------------------------------------------------------------------

func TestAdmin_PushManagedAndRead(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-x", "tok-x", "active")

	// Admin pushes the managed metadata blob for proj-x.
	blob := []byte(`{"services":[{"containers":[{"name":"c1","node_id":9}]}]}`)
	mustStatus(t, e.do("PUT", "/v1/admin/projects/proj-x/managed/metadata", e.adminTok, blob), http.StatusOK)

	// The customer for proj-x can read it back via /v1/managed.
	resp := e.do("GET", "/v1/managed/metadata", "tok-x", nil)
	mustStatus(t, resp, http.StatusOK)
	if !bytes.Equal(readBody(t, resp), blob) {
		t.Fatalf("managed blob mismatch")
	}

	// Admin can delete it.
	mustStatus(t, e.do("DELETE", "/v1/admin/projects/proj-x/managed/metadata", e.adminTok, nil), http.StatusOK)
	mustStatus(t, e.do("GET", "/v1/managed/metadata", "tok-x", nil), http.StatusNotFound)
}

func TestAdmin_TenantCreateAndDelete(t *testing.T) {
	e := newTestEnv(t)

	body, _ := json.Marshal(tenantBody{TokenHash: hashToken("tok-new"), Status: "active"})
	mustStatus(t, e.do("PUT", "/v1/admin/tenants/proj-new", e.adminTok, body), http.StatusOK)

	// The project DB file now exists.
	dbFile := filepath.Join(e.dataDir, "projects", "proj-new.db")
	if _, err := os.Stat(dbFile); err != nil {
		t.Fatalf("project db file not created: %v", err)
	}

	// The new tenant's Bearer now authenticates as a customer.
	mustStatus(t, e.do("PUT", "/v1/db/k", "tok-new", []byte("v")), http.StatusOK)

	// Delete the tenant; the DB file is removed.
	mustStatus(t, e.do("DELETE", "/v1/admin/tenants/proj-new", e.adminTok, nil), http.StatusOK)
	if _, err := os.Stat(dbFile); !os.IsNotExist(err) {
		t.Fatalf("project db file not removed: stat err = %v", err)
	}
	// And the token no longer authenticates.
	mustStatus(t, e.do("GET", "/v1/db/k", "tok-new", nil), http.StatusUnauthorized)
}

func TestAdmin_TenantPut_DuplicateTokenHash_Conflict(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "shared-tok", "active")

	// A different project trying to claim the same token_hash → 409.
	body, _ := json.Marshal(tenantBody{TokenHash: hashToken("shared-tok"), Status: "active"})
	resp := e.do("PUT", "/v1/admin/tenants/proj-b", e.adminTok, body)
	mustStatus(t, resp, http.StatusConflict)
}

func TestAdmin_TenantPut_MissingTokenHash_400(t *testing.T) {
	e := newTestEnv(t)
	resp := e.do("PUT", "/v1/admin/tenants/proj-a", e.adminTok, []byte(`{"status":"active"}`))
	mustStatus(t, resp, http.StatusBadRequest)
}

// TestAdmin_CrossTenantDBRead: admin reads any project's customer_kv (the UI's
// cross-tenant reads), addressed by the explicit project_id in the path.
func TestAdmin_CrossTenantDBRead(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	// Customer A writes; admin reads it cross-tenant by project_id.
	mustStatus(t, e.do("PUT", "/v1/db/users", "tok-a", []byte("A-users")), http.StatusOK)
	resp := e.do("GET", "/v1/admin/projects/proj-a/db/users", e.adminTok, nil)
	mustStatus(t, resp, http.StatusOK)
	if got := string(readBody(t, resp)); got != "A-users" {
		t.Fatalf("admin cross-tenant read = %q", got)
	}

	// Admin can also write and delete cross-tenant.
	mustStatus(t, e.do("PUT", "/v1/admin/projects/proj-a/db/users", e.adminTok, []byte("admin-set")), http.StatusOK)
	if got := string(readBody(t, e.do("GET", "/v1/db/users", "tok-a", nil))); got != "admin-set" {
		t.Fatalf("after admin write, customer reads %q", got)
	}
	mustStatus(t, e.do("DELETE", "/v1/admin/projects/proj-a/db/users", e.adminTok, nil), http.StatusOK)
	mustStatus(t, e.do("GET", "/v1/db/users", "tok-a", nil), http.StatusNotFound)
}

// --- Shim ----------------------------------------------------------------------

func TestShim_ReturnsManagedMetadata(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-x", "tok-x", "active")

	blob := []byte(`{"services":[{"containers":[{"name":"c1","node_id":9}]}]}`)
	mustStatus(t, e.do("PUT", "/v1/admin/projects/proj-x/managed/metadata", e.adminTok, blob), http.StatusOK)

	// monarx GET …/v1/kv/projects/<tok>/metadata?raw=true, Bearer tok-x.
	resp := e.do("GET", "/v1/kv/projects/tok-x/metadata?raw=true", "tok-x", nil)
	mustStatus(t, resp, http.StatusOK)
	if !bytes.Equal(readBody(t, resp), blob) {
		t.Fatalf("shim blob mismatch")
	}
}

// TestShim_BearerIsAuthoritative: the {token} PATH segment is irrelevant. A
// different project's Bearer reading proj-x's metadata gets ITS OWN project's
// metadata (or 404), never proj-x's — even if the path says proj-x's token.
func TestShim_BearerIsAuthoritative(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-x", "tok-x", "active")
	e.provisionTenant("proj-y", "tok-y", "active")

	xBlob := []byte(`{"x":"secret"}`)
	mustStatus(t, e.do("PUT", "/v1/admin/projects/proj-x/managed/metadata", e.adminTok, xBlob), http.StatusOK)
	// proj-y has no metadata pushed.

	// Y's Bearer, but the path carries X's token segment. Authoritative identity
	// is the Bearer (tok-y → proj-y), so Y must NOT see X's blob: proj-y has no
	// metadata → 404. The path segment "tok-x" is ignored.
	resp := e.do("GET", "/v1/kv/projects/tok-x/metadata?raw=true", "tok-y", nil)
	mustStatus(t, resp, http.StatusNotFound)

	// Sanity: give proj-y its own distinct metadata; Y reads ITS OWN, not X's,
	// even though the path still says tok-x.
	yBlob := []byte(`{"y":"mine"}`)
	mustStatus(t, e.do("PUT", "/v1/admin/projects/proj-y/managed/metadata", e.adminTok, yBlob), http.StatusOK)
	resp = e.do("GET", "/v1/kv/projects/tok-x/metadata?raw=true", "tok-y", nil)
	mustStatus(t, resp, http.StatusOK)
	if got := readBody(t, resp); !bytes.Equal(got, yBlob) {
		t.Fatalf("shim returned %q, want proj-y's own blob (Bearer is authoritative)", got)
	}
}

func TestShim_NonRaw_BadRequest(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-x", "tok-x", "active")
	mustStatus(t, e.do("PUT", "/v1/admin/projects/proj-x/managed/metadata", e.adminTok, []byte(`{}`)), http.StatusOK)

	// No ?raw=true → 400 (we never emit Consul's base64 envelope).
	mustStatus(t, e.do("GET", "/v1/kv/projects/tok-x/metadata", "tok-x", nil), http.StatusBadRequest)
	mustStatus(t, e.do("GET", "/v1/kv/projects/tok-x/metadata?raw=false", "tok-x", nil), http.StatusBadRequest)
}

func TestShim_RequiresAuth(t *testing.T) {
	e := newTestEnv(t)
	// No token → 401, even though the path carries a {token} segment.
	mustStatus(t, e.doRaw("GET", "/v1/kv/projects/anything/metadata?raw=true", "", nil), http.StatusUnauthorized)
}

// --- Client errors → 400 (not 500) --------------------------------------------

// TestClientError_EmptyPathPut_400: an empty {path...} on a PUT must be a 400
// (the store's ErrInvalidPath sentinel), not a 500.
func TestClientError_EmptyPathPut_400(t *testing.T) {
	e := newTestEnv(t)
	e.provisionTenant("proj-a", "tok-a", "active")

	// PUT /v1/db/ → {path...} captures "" → store returns ErrInvalidPath → 400.
	resp := e.do("PUT", "/v1/db/", "tok-a", []byte("v"))
	mustStatus(t, resp, http.StatusBadRequest)

	// Same for the admin managed PUT with an empty trailing path.
	resp = e.do("PUT", "/v1/admin/projects/proj-a/managed/", e.adminTok, []byte("v"))
	mustStatus(t, resp, http.StatusBadRequest)
}

// TestClientError_InvalidProjectID_400: a malformed {project_id} on an admin
// data route must be a 400 (the store's ErrInvalidProjectID sentinel), not 500.
// We use a leading-dot id (".bad"), which is path-valid in the URL but rejected
// by the store's validator. (Traversal ids like "../x" can't reach the handler —
// the router/path-cleaning rejects them first.)
func TestClientError_InvalidProjectID_400(t *testing.T) {
	e := newTestEnv(t)

	resp := e.do("PUT", "/v1/admin/projects/.bad/managed/metadata", e.adminTok, []byte("{}"))
	mustStatus(t, resp, http.StatusBadRequest)

	resp = e.do("GET", "/v1/admin/projects/.bad/db/x", e.adminTok, nil)
	mustStatus(t, resp, http.StatusBadRequest)

	resp = e.do("DELETE", "/v1/admin/projects/.bad/db/x", e.adminTok, nil)
	mustStatus(t, resp, http.StatusBadRequest)
}

// TestServerTimeouts_Configured asserts the customer-reachable listener has the
// Slowloris time bounds set (ReadHeaderTimeout + IdleTimeout) and WriteTimeout
// left unset (large value reads are legitimate slow responses).
func TestServerTimeouts_Configured(t *testing.T) {
	srv := New(Config{ListenAddr: "127.0.0.1:0"}, nil, nil)
	if srv.http.ReadHeaderTimeout != 10*time.Second {
		t.Fatalf("ReadHeaderTimeout = %v, want 10s", srv.http.ReadHeaderTimeout)
	}
	if srv.http.IdleTimeout != 120*time.Second {
		t.Fatalf("IdleTimeout = %v, want 120s", srv.http.IdleTimeout)
	}
	if srv.http.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %v, want 0 (unset)", srv.http.WriteTimeout)
	}
}

// --- Body cap -----------------------------------------------------------------

func TestBodyCap_413(t *testing.T) {
	e := newTestEnv(t) // MaxBodyBytes = 1 MiB
	e.provisionTenant("proj-a", "tok-a", "active")

	// Just under the cap: OK.
	ok := bytes.Repeat([]byte("x"), (1<<20)-10)
	mustStatus(t, e.do("PUT", "/v1/db/k", "tok-a", ok), http.StatusOK)

	// Over the cap: 413.
	over := bytes.Repeat([]byte("x"), (1<<20)+1024)
	resp := e.do("PUT", "/v1/db/k", "tok-a", over)
	mustStatus(t, resp, http.StatusRequestEntityTooLarge)
}

// --- Proxy disabled -----------------------------------------------------------

// TestProxyDisabled_UnknownTenant401_NoConsul: with proxy_to_consul off
// (default), an unknown tenant is a flat 401 — there is no Consul dependency on
// the serving path (this test never touches Consul). And an unknown PATH for a
// known tenant is a 404, not a forward.
func TestProxyDisabled_UnknownTenant401_NoConsul(t *testing.T) {
	e := newTestEnv(t) // ProxyToConsul: false
	if e.srv.proxyEnabled() {
		t.Fatal("test env must have the proxy disabled")
	}

	// Unknown tenant → 401, no forward.
	resp := e.do("GET", "/v1/db/anything", "totally-unknown-token", nil)
	mustStatus(t, resp, http.StatusUnauthorized)

	// Known tenant, unknown path → 404, no forward.
	e.provisionTenant("proj-a", "tok-a", "active")
	resp = e.do("GET", "/v1/db/never-written", "tok-a", nil)
	mustStatus(t, resp, http.StatusNotFound)
}

// TestUnit_BearerToken exercises the header parser directly.
func TestUnit_BearerToken(t *testing.T) {
	cases := []struct {
		header   string
		wantTok  string
		wantOK   bool
		caseName string
	}{
		{"Bearer abc", "abc", true, "standard"},
		{"bearer abc", "abc", true, "lowercase scheme"},
		{"BEARER  abc  ", "abc", true, "trimmed"},
		{"Basic abc", "", false, "wrong scheme"},
		{"", "", false, "empty"},
		{"Bearer", "", false, "no space"},
	}
	for _, tc := range cases {
		t.Run(tc.caseName, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			tok, ok := bearerToken(r)
			if ok != tc.wantOK || (ok && tok != tc.wantTok) {
				t.Fatalf("bearerToken(%q) = (%q,%v), want (%q,%v)", tc.header, tok, ok, tc.wantTok, tc.wantOK)
			}
		})
	}
}

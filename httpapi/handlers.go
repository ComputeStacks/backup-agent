package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"cs-agent/store"
)

// --- Customer routes (project comes from sc.projectID, NEVER the URL) --------

// handleCustomerDBGet reads customer_kv[path] for the authenticated tenant.
func (s *Server) handleCustomerDBGet(w http.ResponseWriter, r *http.Request, sc scope) {
	path := r.PathValue("path")
	e, found, err := s.store.CustomerGet(r.Context(), sc.projectID, path)
	if err != nil {
		s.storeError(w, err, "customer get")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeValue(w, e)
}

// handleCustomerDBPut upserts customer_kv[path] for the authenticated tenant.
// Full-value replace. The stored value is NOT size-capped (that is the point of
// the cutover); only the transport body is bounded via http.MaxBytesReader.
func (s *Server) handleCustomerDBPut(w http.ResponseWriter, r *http.Request, sc scope) {
	path := r.PathValue("path")
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	if err := s.store.CustomerPut(r.Context(), sc.projectID, path, r.Header.Get("Content-Type"), body); err != nil {
		s.storeError(w, err, "customer put")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleCustomerDBDelete removes customer_kv[path] for the authenticated tenant.
func (s *Server) handleCustomerDBDelete(w http.ResponseWriter, r *http.Request, sc scope) {
	path := r.PathValue("path")
	if err := s.store.CustomerDelete(r.Context(), sc.projectID, path); err != nil {
		s.storeError(w, err, "customer delete")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleCustomerManagedGet reads managed_kv[path] for the authenticated tenant.
// Read-only: there is intentionally no customer-facing managed WRITE route —
// managed_kv is platform-written, customer-read-only.
func (s *Server) handleCustomerManagedGet(w http.ResponseWriter, r *http.Request, sc scope) {
	path := r.PathValue("path")
	e, found, err := s.store.ManagedGet(r.Context(), sc.projectID, path)
	if err != nil {
		s.storeError(w, err, "managed get")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeValue(w, e)
}

// handleShimMetadata is the single legacy compatibility path (D1): the old
// monarx entrypoint does GET …/v1/kv/projects/{token}/metadata?raw=true.
//
// Authoritative identity is the Bearer (requireCustomer already scoped us to one
// project). The {token} path segment is IGNORED for authorization — a token can
// only ever read its OWN managed metadata regardless of what {token} says. We
// serve managed_kv["metadata"] (the controller-pushed minimal blob, D5) as raw
// JSON. Only ?raw=true is handled (monarx always sends it); anything else is a
// 400 (we never emit Consul's base64-wrapped envelope).
func (s *Server) handleShimMetadata(w http.ResponseWriter, r *http.Request, sc scope) {
	if r.URL.Query().Get("raw") != "true" {
		writeError(w, http.StatusBadRequest, "only ?raw=true is supported by this endpoint")
		return
	}
	e, found, err := s.store.ManagedGet(r.Context(), sc.projectID, "metadata")
	if err != nil {
		s.storeError(w, err, "shim managed get")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "no metadata for this project")
		return
	}
	// monarx pipes the body straight into jq; serve the stored blob verbatim as
	// JSON. The content type the controller pushed wins if set, else default to
	// application/json (the shim only ever serves the JSON metadata blob).
	ct := e.ContentType
	if ct == "" {
		ct = "application/json"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(e.Value)
}

// --- Admin routes (project_id is EXPLICIT in the URL path) -------------------
//
// NOTE: the admin DATA routes (managed/* and db/*) assume the project was
// provisioned via PUT /v1/admin/tenants/{project_id}. They do not gate on the
// tenant row existing: a write to a never-provisioned (but path-valid) id will
// lazily create a stray <project_id>.db. That is intentional for 0a and not a
// security concern — admin is a trusted, per-node scope and the project_id is
// validated by the store (ErrInvalidProjectID → 400 before any file is touched
// for an unsafe id). No gating logic is added here in 0a.

// handleAdminManagedPut writes managed_kv[path] for the path's project. This is
// the controller pushing the minimal managed blob (e.g. path "metadata", D5).
func (s *Server) handleAdminManagedPut(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	path := r.PathValue("path")
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	if err := s.store.ManagedPut(r.Context(), projectID, path, r.Header.Get("Content-Type"), body); err != nil {
		s.storeError(w, err, "admin managed put")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAdminManagedDelete removes managed_kv[path] for the path's project.
func (s *Server) handleAdminManagedDelete(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	path := r.PathValue("path")
	if err := s.store.ManagedDelete(r.Context(), projectID, path); err != nil {
		s.storeError(w, err, "admin managed delete")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAdminDBGet reads customer_kv[path] for any project (the UI's
// cross-tenant reads).
func (s *Server) handleAdminDBGet(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	path := r.PathValue("path")
	e, found, err := s.store.CustomerGet(r.Context(), projectID, path)
	if err != nil {
		s.storeError(w, err, "admin db get")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeValue(w, e)
}

// handleAdminDBPut upserts customer_kv[path] for any project.
func (s *Server) handleAdminDBPut(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	path := r.PathValue("path")
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	if err := s.store.CustomerPut(r.Context(), projectID, path, r.Header.Get("Content-Type"), body); err != nil {
		s.storeError(w, err, "admin db put")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAdminDBDelete removes customer_kv[path] for any project.
func (s *Server) handleAdminDBDelete(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	path := r.PathValue("path")
	if err := s.store.CustomerDelete(r.Context(), projectID, path); err != nil {
		s.storeError(w, err, "admin db delete")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// tenantBody is the JSON body of PUT /v1/admin/tenants/{project_id}.
type tenantBody struct {
	TokenHash string `json:"token_hash"`
	Status    string `json:"status"`
}

// handleAdminTenantPut provisions/re-provisions a tenant: UpsertTenant then
// CreateProjectDB. The project_id is taken from the path; the token_hash + status
// from the JSON body. (token_hash, never a plaintext token, is what the store
// records.)
func (s *Server) handleAdminTenantPut(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	var tb tenantBody
	if err := json.Unmarshal(body, &tb); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if tb.TokenHash == "" {
		writeError(w, http.StatusBadRequest, "token_hash is required")
		return
	}
	if err := s.store.UpsertTenant(r.Context(), projectID, tb.TokenHash, tb.Status); err != nil {
		if errors.Is(err, store.ErrTenantExists) {
			writeError(w, http.StatusConflict, "token_hash already mapped to a different project")
			return
		}
		s.storeError(w, err, "upsert tenant")
		return
	}
	if err := s.store.CreateProjectDB(r.Context(), projectID); err != nil {
		s.storeError(w, err, "create project db")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAdminTenantDelete de-provisions a tenant: DeleteTenant then
// DeleteProjectDB. Both are idempotent no-ops on an absent project.
func (s *Server) handleAdminTenantDelete(w http.ResponseWriter, r *http.Request, _ scope) {
	projectID := r.PathValue("project_id")
	if err := s.store.DeleteTenant(r.Context(), projectID); err != nil {
		s.storeError(w, err, "delete tenant")
		return
	}
	if err := s.store.DeleteProjectDB(r.Context(), projectID); err != nil {
		s.storeError(w, err, "delete project db")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- helpers -----------------------------------------------------------------

// readBody reads the request body under the configured cap. On a body that
// exceeds MaxBodyBytes, http.MaxBytesReader surfaces a *http.MaxBytesError; we
// translate that to 413 and return ok=false (the handler must stop). The stored
// VALUE has no size cap — only this transport-level body limit applies.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "error reading request body")
		return nil, false
	}
	return body, true
}

// storeError maps a store error onto an HTTP status:
//   - ErrProjectDeleting   → 404 (the project is gone);
//   - ErrInvalidPath /
//     ErrInvalidProjectID  → 400 (client error: empty {path...} on a PUT, or a
//     malformed {project_id} on an admin route — surfaced via the store
//     sentinels rather than duplicating validation in the handlers);
//   - anything else        → logged 500.
//
// Auth-time ErrProjectDeleting is handled in authenticate; this covers the
// per-request data path.
func (s *Server) storeError(w http.ResponseWriter, err error, op string) {
	switch {
	case errors.Is(err, store.ErrProjectDeleting):
		writeError(w, http.StatusNotFound, "project is gone")
	case errors.Is(err, store.ErrInvalidPath):
		writeError(w, http.StatusBadRequest, "invalid path")
	case errors.Is(err, store.ErrInvalidProjectID):
		writeError(w, http.StatusBadRequest, "invalid project_id")
	default:
		s.log.Error("store operation failed", "op", op, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// writeValue writes a stored KV entry back to the client, preserving the stored
// content type when present.
func writeValue(w http.ResponseWriter, e store.KVEntry) {
	if e.ContentType != "" {
		w.Header().Set("Content-Type", e.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(e.Value)
}

// writeError writes a small JSON error body with the given status.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

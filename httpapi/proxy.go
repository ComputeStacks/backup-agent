package httpapi

// Proxy-to-Consul leg — SCAFFOLD, DEFAULT OFF. BLOCKER: do not enable.
//
// The cutover design eventually has the agent be the
// single front door on :8500 and proxy reads for not-yet-migrated projects
// through to the relocated Consul HTTP port via cnslclient. That dual-run leg is
// NOT wired here on purpose:
//
//   - Config.ProxyToConsul defaults to FALSE (see config.go / agent.sample.yml).
//   - While false, authenticate() returns 401 for an unknown tenant and the
//     handlers return 404 for an unknown path. There is NO forward to Consul, so
//     there is no Consul dependency on the serving path and no way for an
//     un-authenticated/cross-tenant request to reach Consul's KV.
//   - A project is "served by the agent" exactly when its tenant row exists
//     (migrate a project → UpsertTenant → it is served). Everything else is a
//     miss, full stop, until this leg is implemented.
//
// The open blocker is wire-auth (still open): how an
// incoming `Authorization: Bearer <token>` maps onto Consul's `X-Consul-Token`
// at the relocated HTTP port. Forwarding cross-tenant reads before that is
// resolved would be a tenant-isolation hole, so the forward stays unbuilt.
//
// TODO(proxy wire-auth): once Bearer→X-Consul-Token is decided, implement the
// forward here (use cnslclient.Client() against the relocated Consul HTTP port),
// call it from authenticate()/the GET handlers ONLY when proxyEnabled() is true,
// and add the dual-run isolation tests. Until then this function is a no-op stub
// that the build references so the gate is obvious and the leg can't be wired by
// accident elsewhere.
func (s *Server) proxyEnabled() bool {
	return s.cfg.ProxyToConsul
}

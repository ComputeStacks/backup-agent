# cs-agent — native packaging & apt distribution

Production runs the agent as a **native binary under systemd**, installed from a
self-hosted, GPG-signed apt repo on S3. The container build (`Dockerfile`) is kept
for local dev only.

## Release flow (automated)

`git tag vX.Y.Z && git push --tags` → `.github/workflows/release.yml`:

1. GoReleaser builds amd64+arm64, packages the `.deb` (nfpm), and cuts the GitHub
   Release with notes extracted from `CHANGELOG.md`.
2. `apt-publish pull` downloads the existing `pool/` from S3.
3. The new `.deb`s are dropped into the pool; `build-apt-repo.sh` rebuilds the
   index from the **full** pool and signs `Release`/`InRelease`.
4. `apt-publish push` uploads the repo (signed Release files last).

The pool is **append-only** (every version stays installable → rollback); the
index is a regenerable function of the pool (self-healing).

## One-time setup

**GitHub Actions — Environment `release`** (add required reviewers):

| Kind | Name | Notes |
|---|---|---|
| secret | `GPG_PRIVATE_KEY` | dedicated **signing subkey**, ASCII-armored (not the master key) |
| secret | `GPG_PASSPHRASE` | passphrase for the subkey |
| secret | `APT_S3_ACCESS_KEY` | S3 access key (→ `AWS_ACCESS_KEY_ID`) |
| secret | `APT_S3_SECRET_KEY` | S3 secret key (→ `AWS_SECRET_ACCESS_KEY`) |
| var | `APT_S3_ENDPOINT` | S3 endpoint, e.g. `https://s3.example.com` |
| var | `APT_S3_REGION` | e.g. `us-east-1` |
| var | `APT_S3_BUCKET` | shared **public-read** bucket for all ComputeStacks OSS packages (e.g. `cs-packages`); separate from the private backup bucket |
| var | `APT_S3_PREFIX` | **shared** repo root — the **same** for every package (empty = bucket root, or e.g. `apt/`); all packages publish into the one `dists/`+`pool/` |

**S3 bucket (one shared repo):** a **single** apt repo serves every ComputeStacks **OSS**
package — one `dists/` index over one `pool/` holding all packages — signed with **one
shared key**. Nodes add it **once** and `apt install <any-cs-package>`; new packages need
no node change. Make the bucket **anonymously readable over HTTPS** via a **bucket policy**
(not per-object ACLs — some S3-compatible stores handle those differently); fits the
existing bucket-setup tooling.

> **Public bucket = OSS packages only.** Keep any proprietary/internal package out of it
> (distribute those via GitLab or a private, credentialed repo). cs-agent is OSS → fine here.

## Node setup (install + upgrade)

Dropped **once** at provisioning — never touched again, even as new packages ship:

```sh
# one shared keyring for ALL ComputeStacks packages
curl -fsSL https://<base>/computestacks.gpg.asc | gpg --dearmor \
  | sudo tee /etc/apt/keyrings/computestacks.gpg >/dev/null

# ONE source line for the whole ComputeStacks repo
echo "deb [signed-by=/etc/apt/keyrings/computestacks.gpg] https://<base> stable main" \
  | sudo tee /etc/apt/sources.list.d/computestacks.list

sudo apt-get update && sudo apt-get install -y cs-agent          # ...or: cs-agent foo-package
```

`<base>` = the public HTTPS URL for the shared repo (e.g. `https://pkg.computestacks.com`
fronting `s3://cs-packages` at `APT_S3_PREFIX`).

Bucket layout (one index, one pool, many packages):

```
s3://cs-packages/                 (public-read; one shared signing key)
  dists/stable/main/binary-{amd64,arm64}/Packages   ← single shared index
  dists/stable/{Release,InRelease,Release.gpg}
  pool/main/c/cs-agent/cs-agent_*.deb
  pool/main/f/foo-package/foo-package_*.deb          ← future packages just join the pool
```

## Scaling to multiple packages (the shared-repo tooling)

Today **cs-agent's `release.yml` is the sole publisher** of the shared index, so it's
race-free. Two pieces keep it that way as the repo grows:

- **`reconcile-apt-repo.yml`** (scheduled + manual) rebuilds the index from the **full
  pool** and re-signs, on the same `apt-publish` concurrency group as releases. This
  guarantees the index always matches the pool — self-healing any partial publish — and is
  the seed of the single index-builder below.
- **When package #2 ships** (a second, independent CI): do **not** have it rebuild + sign
  the shared index too — that races on the index and copies the signing key into a second
  CI. Instead, package CIs only **upload their `.deb` to the shared `pool/`**, and a
  **single index-builder** (one workflow, holding the *only* copy of the signing key)
  rebuilds + signs + publishes the index — triggered by the package CIs (`repository_dispatch`)
  and/or the reconcile cron. Race-free, and the key lives in exactly one place.
- **Decision for then:** where that index-builder + key live. Recommend a **dedicated infra
  repo** (not the public cs-agent repo) — especially since some future packages are internal
  (GitLab), and they'd dispatch to a neutral builder rather than into an OSS GitHub repo.

Updates are just `apt-get update && apt-get upgrade` (per node today; fleet-wide
via Ansible later).

## Rollback

Every version stays in the pool:

```sh
apt list -a cs-agent                          # see all available versions
sudo apt-get install --allow-downgrades cs-agent=1.9.0
sudo apt-mark hold cs-agent                   # pin so `upgrade` won't move it back up
```

> **Rollback safety:** the binary rolls back trivially, but the agent runs SQLite
> migrations on boot. Rollback is only safe because migrations are
> **additive-by-default + schema-version-guarded**. Don't
> roll back across a non-additive migration without the matching down-migration.

## Status / caveats

- The systemd unit ships with `Type=simple`. `Type=notify` + `WatchdogSec` are
  commented out until the agent implements `sd_notify`.
- `apt-publish` + the S3 interaction (path-style, public-read, checksums) need a
  **test pass against the real S3 endpoint** before first production use.
- Validate the `.deb` `Depends:` package names (`borgbackup`, `iptables`,
  `ca-certificates`) on Debian 12/13.

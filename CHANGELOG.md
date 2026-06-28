# Changelog

## v2.0.0-rc1

Major release — the agent becomes the node's **data plane** (part of the Consul-retirement /
node-autonomy re-architecture). Three independent changes ship together; production rolls out staged
(native deploy first, then the firewall and metadata cutovers, validated on a canary).

- [CHANGE] **Native deployment.** The agent now runs as a **native systemd binary installed from a
  self-hosted, GPG-signed apt repo**, replacing the `docker run` container unit. The container image
  is kept for local dev only. `cs-agent -version` reports the build version/commit/date.
- [CHANGE] **nftables firewall.** Published-port DNAT/forwarding is rendered into a native `cs_agent`
  nftables table via netlink, replacing the iptables shell-out + string-diff. Cross-project isolation
  stays in `DOCKER-USER`. **Fail-closed:** published ports are closed until the first reconcile. Reads
  the same `ingress_rules` desired state. (Relies on the project bridges' `nat-unprotected` mode, under
  which Docker already accepts the forwarded ingress.)
- [FEATURE] **Customer metadata served by the agent.** A new HTTP API on `node.primary_ip:8500` serves
  per-project customer metadata from **embedded SQLite** — no more Consul KV for the `/db/` space, and
  **no value size cap** (kills the 512 KB ceiling). Bearer→tenant auth + a per-node admin Bearer; a
  compatibility shim serves the legacy `…/metadata?raw=true` read. Migrations are rollback-tolerant
  (additive + schema-version guard).

### Upgrading a node to v2.0.0

Take a maintenance window — the firewall cutover (+ optional reboot) briefly closes published ports.
All nodes must be **Debian 12/13** (`iptables` = the nft backend). Snapshot the firewall first:
`iptables-save > /root/iptables.pre-upgrade`.

1. **Native binary** — add the apt source + keyring, stop the old container unit, install:
   ```
   curl -fsSL https://repo.computestacks.com/public/computestacks.gpg.asc \
     | gpg --dearmor | sudo tee /etc/apt/keyrings/computestacks.gpg >/dev/null
   echo "deb [signed-by=/etc/apt/keyrings/computestacks.gpg] https://repo.computestacks.com/public stable main" \
     | sudo tee /etc/apt/sources.list.d/computestacks.list
   sudo systemctl disable --now cs-agent; docker rm -f cs-agent 2>/dev/null || true
   sudo rm -f /etc/systemd/system/cs-agent.service   # the package unit lives in /lib/systemd/system
   sudo apt-get update && sudo apt-get install -y cs-agent
   ```
2. **Metadata / Consul port** — the agent binds `:8500`, so Consul's HTTP listener moves to `:8502`
   (provisioner); confirm the agent's `consul.host` + the admin-token hash are configured. New
   containers receive `CS_NODE_ID`; existing ones use the compatibility shim — no recreation needed.
3. **Firewall** — the agent renders the `cs_agent` nft table on start (`nft list table ip cs_agent`).
   The host firewall itself is applied at boot by `cs-iptables.service` (a oneshot that runs
   `/usr/local/bin/cs-recover_iptables`); the agent does not manage that file. **Edit that file
   directly** to delete the lines the agent has now taken over — the `10000:50000` INPUT range and
   the `expose-ports`/`container-inbound` chain setup — then **reboot**
   so the oneshot re-applies the trimmed ruleset from a clean slate (or, to avoid a reboot, delete
   those rules from the live ruleset by hand). Verify published ports still reach containers and
   `iptables -S` shows none of the old `expose-ports`/`container-inbound`/`10000:50000` artifacts.
   - **Rollback** — v2.0.0 is the *first* native release, so there is **no previous `.deb`**; the
     prior version ran as a Docker container, so rolling back means undoing the deployment-model
     change, not just downgrading a package:
       1. `sudo apt-get purge cs-agent` (removes the native binary + the `/lib/systemd/system` unit).
       2. Restore the old containerized `cs-agent.service` (the `docker run` unit) and pull the agent
          image — i.e. re-apply the previous provisioner config.
       3. Restore the host firewall: `sudo iptables-restore < /root/iptables.pre-upgrade`, **and**
          revert `/usr/local/bin/cs-recover_iptables` to the version that re-creates the
          `expose-ports`/`container-inbound` chains + the `10000:50000` range — the old containerized
          agent *appends* to those chains and silently loses published ports without them.
       4. Re-bind Consul's HTTP listener to `:8500` (the old agent and customer containers reach
          metadata via Consul there).
     From v2.0.1 onward rollback is a normal `apt-get install --allow-downgrades cs-agent=<prev>`;
     never roll back across a non-additive DB migration.

The controller/provisioner changes (`CS_NODE_ID` injection, the Consul port move, the host-firewall
trim, the apt source) ship alongside — coordinate per the rollout runbook.

## v1.10.0

- [FEATURE] Backup export ("download backup"): a new `backup.export` job streams a chosen archive to S3 (or S3-compatible storage) via `borg export-tar --bypass-lock` and publishes a presigned download URL to Consul KV (`borg/exports/<volume>/<jid>`) for ComputeStacks to read. Streams with no scratch disk, runs on a dedicated worker so it never blocks scheduled backups, and serializes against compaction per-repo. Configure under `backups.export.*`; inert until `backups.export.s3.bucket` is set. NOTE: the exported tar is plaintext (unlike the encrypted repo) — keep the bucket private, enable SSE, and use a short URL TTL + object-expiry lifecycle rule.

## v1.9.0

- [CHANGE] Move borg repository compaction from the backup server's host cron into the agent. It is scheduled per node (`backups.compact_freq`, with `backups.compact_jitter_sec` to spread load across nodes) and serialized against exports/prune via a per-volume lock. NFS-backed repositories are compacted locally on the server over SSH (`backups.borg.nfs_borg_path`). **Operators: remove the `cs-borg_compact` host cron once the fleet is upgraded; stagger the two during the overlap.**
- [FIX] Only advance a volume's `last_backup` timestamp when the backup actually succeeds. Previously it advanced even on failure, masking missed backups.
- [CHANGE] `borg create` now waits out an in-progress compact/prune (`backups.borg.lock_wait_create`, default 600s) rather than failing after 1 second and missing the backup.
- [CHANGE] Surface remote stderr from SSH commands (e.g. failed NFS compaction/chown) in agent logs.

## v1.8.0

- [CHANGE] Move docker network isolation under the responsibility of the backup agent.
- [FIX] Resolve crash during firewall reconciliation on nodes with no ingress rules defined.
- [FIX] Resolve crash when backing up a MySQL/MariaDB container that is offline or whose project event could not be created.
- [FIX] Resolve crash while stopping a backup container that failed to initialize.

## 1.7.0

* [CHANGE] Support for docker api v1.44
* [CHANGE] Refactor and update dependencies.

## 1.6.0

* [CHANGE] Bump dependencies to new major versions.
* [CHANGE] Support for Mariadb 11.

## 1.5.2

* [CHANGE] Include ssl in the docker image.

***

## 1.5.1

* [CHANGE] Our agent will now run inside of a container by default.

***

## 1.5.0

* [FEATURE] Support creating iptable rules for linux bridges.

***

## 1.4.2

* [FIX] Resolve issue that prevented cloning from an ssh target.
* [FIX] Fix cleaning up the backup folder when backing up MariaDB and MySQL containers.

***

## 1.4.1

* [CHANGE] make --lock-wait configurable in the yaml file.

***

## 1.4.0

* [FEATURE] Support backing up over SSH as an alternative to NFS.

***

## 1.3.6

* [FIX] Resolved error handling response from borg during volume deletion.

***

## 1.3.5

* [FIX] Resolve an issue that prevented volumes with backups disabled from being cloned.

***

## 1.3.4

* [CHANGE] Update system container images to use GitHub registry to avoid rate limits with Docker Hub.
* [CHANGE] Update borg to use `--numeric-ids` instead of the deprecated `--numeric-owner`.

***

## 1.3.3

* [CHANGE] MariaDB will use built-in MariaBackup, rather than a separate container.
* [FIX] Resolve MariaDB backup issues with v10.6+.

***

## 1.3.2

* [CHANGE] Additional tuning parameters available for MariaBackup.

***

## 1.3.1

* [CHANGE] Add in placeholder for MariaDB 10.10 that's in development.
* [CHANGE] Beginning with MariaDB 10.9, the container no longer includes the `MARIADB_MAJOR` environmental parameter, which we used to determine which version of maria backup to use. There is a request in with the MariaDB docker developer to add that back in, but for now we're defaulting to v10.9 if the `MAJOR` param is missing, but `VERSION` exists.

***

## 1.3.0

* [FEATURE] Restore backup from different volume.
* [CHANGE] Build arm64 binaries.
* [CHANGE] Configurable option to create nfs directory.
* [CHANGE] Issue `CHECKPOINT` command to postgres before taking snapshot.
* [CHANGE] Improvements to how container restores happen.
* [FIX] Resolve issue that left mysql/mariadb backup containers running after a restore.

***

## 1.2.5

* [FIX] Incorrectly flagged MySQL 5.6 as v8+.

***

## 1.2.4

* [CHANGE] Add in support for parsing the mariadb version used in a bitnami image.
* [FIX] Add additional checks to avoid nil pointer dereference when loading volume data from consul.

***

## 1.2.3

* [FEATURE] Support for consul token auth.

***

## 1.2.1

* [CHANGE] Hooks will now require at least 3 characters before executing.
* [FIX] Duplicate volume IDs in event log.

***

## 1.2.0

* [FEATURE] Support for excluding cache directories. `echo "Signature: 8a477f597d28d172789f06886806bc55" > CACHEDIR.TAG`

***

## 1.1.0

* [FEATURE] IPTable rules are now stored in consul, instead of having to poll the controller.

***

## 1.0.0

* [FEATURE] Manage iptables for udp container rules, and sftp containers.

***

## 0.4.0

* [FEATURE] Support for Bitnami's MariaDB using our MySQL backup tool.
* [CHANGE] A backup volume is created per-repository, and will auto-mount via NFS if applicable.
* [CHANGE] MySQL Backup jobs for offline containers will now show as "cancelled", and not "failed", in ComputeStacks.

***

## v0.3.1

* [FIX] Prune events will correctly stop their container after running.
* [FIX] Prune will correctly find the repo, and halt if it does not exist.

***

## v0.3.0

* [CHANGE] Restore will now completely clear the volume before restoring.

***

## v0.2.0

* [CHANGE] Uses docker container for backing up, instead of host system.
* [FIX] Various bug fixes

***

### Oct 19, 2020

* [CHANGE] Package Updates.
* [FIX] Resolve nil pointer error on posting events to ComputeStacks.

***

## v0.1.4

### June 27th, 2019

* [FIX] Incorrect parameters being passed to `borg prune`.

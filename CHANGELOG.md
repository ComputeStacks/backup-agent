# Changelog

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

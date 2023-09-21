# ComputeStacks Node Agent

This agent runs on each node and uses consul as it's database backend. It currently performs the following purpose:

* Manage volume backups
* Creates firewall rules for container services
* Performs volume restores

## Running

```bash
docker run -d --init --network host -v /var/run/docker.sock:/var/run/docker.sock -v /etc/computestacks:/etc/computestacks:ro --name cs-agent cs-agent:latest
```

## Development

Install required go packages with `go mod download`

**NOTE:** Do not run `go get -u` to update `go modules`. Docker has not used semantic versioning in quite some time, so running the update all command will replace the current docker api with an older version, thus breaking everything.
Update modules with `go get <mod>@<version>` instead.


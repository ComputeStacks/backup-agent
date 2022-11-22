# ComputeStacks Node Agent

This agent runs on each node and uses consul as it's database backend. It currently performs the following purpose:

* Manage volume backups
* Creates firewall rules for container services
* Performs volume restores

## Upgrading

To upgrade your backup agent:

```bash
systemctl stop cs-agent
cd /tmp && wget https://f.cscdn.cc/file/cstackscdn/packages/cs-agent/cs-agent.tar.gz
tar -xzvf cs-agent.tar.gz
rm -f /usr/local/bin/cs-agent
mv cs-agent /usr/local/bin/
chown root:root /usr/local/bin/cs-agent && chmod +x /usr/local/bin/cs-agent
rm -rf /tmp/cs-agent*
systemctl daemon-reload && systemctl start cs-agent
```



## Development

Install required go packages with `go mod download`

**NOTE:** Do not run `go get -u` to update `go modules`. Docker has not used semantic versioning in quite some time, so running the update all command will replace the current docker api with an older version, thus breaking everything.
Update modules with `go get <mod>@<version>` instead.


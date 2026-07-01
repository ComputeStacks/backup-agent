package firewall

import (
	"github.com/spf13/viper"
)

// iptablesCmd selects the iptables binary used by the cross-project isolation
// path in isolation.go (which still shells iptables in DOCKER-USER). The
// published-port path no longer shells iptables -- it renders the native
// cs_agent nftables table directly via netlink (nftable.go / nftable_apply.go)
// -- so the only remaining shell-out is isolation. The host.iptables-cmd toggle
// stays for that path.
func iptablesCmd() string {
	if viper.GetString("host.iptables-cmd") == "iptables-legacy" {
		return "iptables-legacy"
	}
	return "iptables"
}

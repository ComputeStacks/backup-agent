package cnslclient

import (
	consulAPI "github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
)

func Client() (*consulAPI.Client, error) {
	if viper.GetBool("consul.tls") {
		consulTLSConfig := consulAPI.TLSConfig{
			Address:            viper.GetString("consul.host"),
			CAFile:             "/etc/computestacks/certs/consul/ca.crt",
			CertFile:           "/etc/computestacks/certs/consul/client.crt",
			KeyFile:            "/etc/computestacks/certs/consul/client.key",
			InsecureSkipVerify: false,
		}
		return consulAPI.NewClient(&consulAPI.Config{
			Address:   viper.GetString("consul.host"),
			Scheme:    "https",
			TLSConfig: consulTLSConfig,
			Token:     viper.GetString("consul.token"),
		})
	}
	return consulAPI.NewClient(&consulAPI.Config{
		Address: viper.GetString("consul.host"),
		Scheme:  "http",
		Token:   viper.GetString("consul.token"),
	})
}

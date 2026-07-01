package firewall

import (
	"net"
	"reflect"
	"testing"
)

// ports is a small helper to make the expectations readable.
func ports(p ...uint16) []uint16 {
	if len(p) == 0 {
		return nil
	}
	return p
}

func dnat(natPort uint16, ip string, destPort uint16) dnatEntry {
	return dnatEntry{NatPort: natPort, DestIP: net.ParseIP(ip).To4(), DestPort: destPort}
}

// TestBuildPlan covers the pure desired-state -> nft-objects construction: the
// tcp/udp split, multiple ports, dedup, dest/proto skipping, the empty/nil ->
// empty-table fail-closed default, and that the Driver field is a no-op for the
// DNAT-only cs_agent table. No kernel.
func TestBuildPlan(t *testing.T) {
	cases := []struct {
		name  string
		rules *NatRules
		want  *renderPlan
	}{
		{
			name:  "nil rules -> empty table (fail-closed)",
			rules: nil,
			want: &renderPlan{
				TCP: protoPlan{Proto: protoTCP},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name:  "empty ruleset -> empty table",
			rules: &NatRules{},
			want: &renderPlan{
				TCP: protoPlan{Proto: protoTCP},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "single tcp rule -> published + dnat",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 23456, Port: 80, Dest: "10.100.0.5", Driver: "bridge"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(23456),
					DNAT:      []dnatEntry{dnat(23456, "10.100.0.5", 80)},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "tcp and udp rules land in their own proto plans",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 20000, Port: 443, Dest: "10.100.0.5"},
				{Proto: "udp", Nat: 20001, Port: 53, Dest: "10.100.0.6"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(20000),
					DNAT:      []dnatEntry{dnat(20000, "10.100.0.5", 443)},
				},
				UDP: protoPlan{
					Proto:     protoUDP,
					Published: ports(20001),
					DNAT:      []dnatEntry{dnat(20001, "10.100.0.6", 53)},
				},
			},
		},
		{
			name: "multiple tcp ports are sorted by nat port",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 30000, Port: 8080, Dest: "10.100.0.9"},
				{Proto: "tcp", Nat: 10000, Port: 80, Dest: "10.100.0.7"},
				{Proto: "tcp", Nat: 20000, Port: 443, Dest: "10.100.0.8"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(10000, 20000, 30000),
					DNAT: []dnatEntry{
						dnat(10000, "10.100.0.7", 80),
						dnat(20000, "10.100.0.8", 443),
						dnat(30000, "10.100.0.9", 8080),
					},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "Driver is a no-op: calico-node rule still contributes published + dnat",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 25000, Port: 80, Dest: "10.100.0.5", Driver: "calico-node"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(25000),
					DNAT:      []dnatEntry{dnat(25000, "10.100.0.5", 80)},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "all drivers treated identically (no forward chain -> Driver irrelevant)",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 25000, Port: 80, Dest: "10.100.0.5", Driver: "calico_docker"},
				{Proto: "tcp", Nat: 25001, Port: 81, Dest: "10.100.0.6", Driver: "calico-node"},
				{Proto: "tcp", Nat: 25002, Port: 82, Dest: "10.100.0.7", Driver: ""},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(25000, 25001, 25002),
					DNAT: []dnatEntry{
						dnat(25000, "10.100.0.5", 80),
						dnat(25001, "10.100.0.6", 81),
						dnat(25002, "10.100.0.7", 82),
					},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "duplicate identical rules collapse",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 21000, Port: 80, Dest: "10.100.0.5"},
				{Proto: "tcp", Nat: 21000, Port: 80, Dest: "10.100.0.5"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(21000),
					DNAT:      []dnatEntry{dnat(21000, "10.100.0.5", 80)},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "unknown proto is skipped",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "sctp", Nat: 22000, Port: 80, Dest: "10.100.0.5"},
				{Proto: "tcp", Nat: 22001, Port: 80, Dest: "10.100.0.5"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(22001),
					DNAT:      []dnatEntry{dnat(22001, "10.100.0.5", 80)},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
		{
			name: "non-IPv4 / unparseable dest is skipped",
			rules: &NatRules{Rules: []NatRule{
				{Proto: "tcp", Nat: 23000, Port: 80, Dest: "not-an-ip"},
				{Proto: "tcp", Nat: 23001, Port: 80, Dest: "fd00::1"},
				{Proto: "tcp", Nat: 23002, Port: 80, Dest: "10.100.0.5"},
			}},
			want: &renderPlan{
				TCP: protoPlan{
					Proto:     protoTCP,
					Published: ports(23002),
					DNAT:      []dnatEntry{dnat(23002, "10.100.0.5", 80)},
				},
				UDP: protoPlan{Proto: protoUDP},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := buildPlan(c.rules)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("buildPlan mismatch\n got: %+v\nwant: %+v", got, c.want)
			}
		})
	}
}

// The DNAT map value must be the 8-byte concat ipv4_addr(4) . inet_service(2,
// padded to 4): addr in the first register, port in the second. The NAT
// expression in dnatRule references RegAddrMin=1, RegProtoMin=2, so the byte
// layout here is load-bearing.
func TestDNATElements(t *testing.T) {
	elems, err := dnatElements([]dnatEntry{dnat(0x1234, "10.100.0.5", 0x0050)})
	if err != nil {
		t.Fatalf("dnatElements error: %v", err)
	}
	if len(elems) != 1 {
		t.Fatalf("got %d elements, want 1", len(elems))
	}
	// key = nat port, big-endian u16
	if want := []byte{0x12, 0x34}; !reflect.DeepEqual(elems[0].Key, want) {
		t.Errorf("key = % x, want % x", elems[0].Key, want)
	}
	// value = 10.100.0.5 (4) + port 0x0050 (2) + pad (2)
	want := []byte{10, 100, 0, 5, 0x00, 0x50, 0x00, 0x00}
	if !reflect.DeepEqual(elems[0].Val, want) {
		t.Errorf("val = % x, want % x", elems[0].Val, want)
	}
}

func TestPortElements(t *testing.T) {
	if got := portElements(nil); got != nil {
		t.Errorf("portElements(nil) = %v, want nil", got)
	}
	elems := portElements([]uint16{0x0102, 0x0304})
	want := [][]byte{{0x01, 0x02}, {0x03, 0x04}}
	if len(elems) != len(want) {
		t.Fatalf("got %d elements, want %d", len(elems), len(want))
	}
	for i := range elems {
		if !reflect.DeepEqual(elems[i].Key, want[i]) {
			t.Errorf("element %d key = % x, want % x", i, elems[i].Key, want[i])
		}
	}
}

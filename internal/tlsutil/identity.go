package tlsutil

import (
	"fmt"
	"net/url"
	"strings"
)

const TrustDomain = "spiffe://krm"

type Role string

const (
	RoleSource Role = "source"
	RoleTarget Role = "target"
)

type Identity struct {
	RunNamespace string
	RunName      string
	Role         Role
	Namespace    string
	Name         string
}

func SourceIdentity(runNamespace, runName, sourceNamespace, sourceName string) Identity {
	return Identity{RunNamespace: runNamespace, RunName: runName, Role: RoleSource, Namespace: sourceNamespace, Name: sourceName}
}

func TargetIdentity(runNamespace, runName, targetNamespace, targetName string) Identity {
	return Identity{RunNamespace: runNamespace, RunName: runName, Role: RoleTarget, Namespace: targetNamespace, Name: targetName}
}

func (i Identity) URI() string {
	return fmt.Sprintf("%s/run/%s/%s/%s/%s/%s", TrustDomain, escape(i.RunNamespace), escape(i.RunName), i.Role, escape(i.Namespace), escape(i.Name))
}

func ParseIdentity(raw string) (Identity, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Identity{}, err
	}
	if u.Scheme != "spiffe" || u.Host != "krm" {
		return Identity{}, fmt.Errorf("unsupported trust domain %q", raw)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 6 || parts[0] != "run" {
		return Identity{}, fmt.Errorf("invalid identity path %q", u.Path)
	}
	role := Role(parts[3])
	if role != RoleSource && role != RoleTarget {
		return Identity{}, fmt.Errorf("invalid identity role %q", role)
	}
	return Identity{
		RunNamespace: unescape(parts[1]),
		RunName:      unescape(parts[2]),
		Role:         role,
		Namespace:    unescape(parts[4]),
		Name:         unescape(parts[5]),
	}, nil
}

func escape(value string) string {
	return url.PathEscape(value)
}

func unescape(value string) string {
	decoded, err := url.PathUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

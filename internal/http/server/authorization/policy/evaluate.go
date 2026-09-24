package policy

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
)

func wildcard(pattern, value string, ignoreCase bool) bool {
	quoted := regexp.QuoteMeta(pattern)
	quoted = strings.ReplaceAll(strings.ReplaceAll(quoted, `\*`, `.*`), `\?`, `.`)
	if ignoreCase {
		quoted = "(?i:" + quoted + ")"
	}
	ok, _ := regexp.MatchString("^(?:"+quoted+")$", value)
	return ok
}

func (s *Snapshot) AuthorizeRequest(ctx context.Context, r *authorization.Request) (authorization.Decision, error) {
	// DeleteObjects is only a container operation. The server performs the real
	// DeleteObject/DeleteObjectVersion authorization separately for every entry.
	if r.Operation == authorization.OperationDeleteObjects {
		return authorization.Decision{Effect: authorization.Allow}, nil
	}
	checks, err := checksFor(r)
	if err != nil {
		return authorization.Decision{Effect: authorization.ImplicitDeny}, err
	}
	statements := s.bySubject["anonymous"]
	if r.Authorization.AccountId != nil && r.Authorization.PrincipalId != nil {
		statements = s.bySubject["principal\x00"+*r.Authorization.AccountId+"\x00"+*r.Authorization.PrincipalId]
	}
	result := authorization.Decision{Effect: authorization.Allow}
	for _, c := range checks {
		d, err := evaluateCheck(ctx, statements, r, c)
		if err != nil {
			return d, err
		}
		if d.Effect == authorization.ExplicitDeny {
			return d, nil
		}
		if d.Effect == authorization.ImplicitDeny {
			result = d
		} else if result.Effect == authorization.Allow {
			result.References = append(result.References, d.References...)
			result.Action = c.action
			result.Resource = c.resource
		}
	}
	return result, nil
}
func evaluateCheck(ctx context.Context, ss []compiledStatement, r *authorization.Request, c check) (authorization.Decision, error) {
	d := authorization.Decision{Effect: authorization.ImplicitDeny, Action: c.action, Resource: c.resource}
	allowed := false
	for _, s := range ss {
		if !matchesAny(s.actions, c.action, true) || !matchesAny(s.resources, c.resource, false) {
			continue
		}
		ok, err := conditionsMatch(ctx, s.conditions, r, c.source)
		if err != nil {
			return d, err
		}
		if !ok {
			continue
		}
		ref := authorization.StatementReference{Policy: s.policy, Sid: s.sid}
		if s.effect == "Deny" {
			d.Effect = authorization.ExplicitDeny
			d.References = append(d.References, ref)
			return d, nil
		}
		allowed = true
		d.References = append(d.References, ref)
	}
	if allowed {
		d.Effect = authorization.Allow
	}
	return d, nil
}
func matchesAny(patterns []string, v string, ignoreCase bool) bool {
	for _, p := range patterns {
		if wildcard(p, v, ignoreCase) {
			return true
		}
	}
	return false
}

func conditionsMatch(ctx context.Context, conditions []condition, r *authorization.Request, source bool) (bool, error) {
	var tags map[string]string
	resolved := false
	for _, c := range conditions {
		vals, present, err := contextValues(ctx, c.key, r, source, &tags, &resolved)
		if err != nil {
			return false, err
		}
		if !present && strings.HasSuffix(c.operator, "IfExists") {
			continue
		}
		ok, err := compareCondition(c.operator, vals, c.values, present)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}
func contextValues(ctx context.Context, key string, r *authorization.Request, source bool, tags *map[string]string, resolved *bool) ([]string, bool, error) {
	now := time.Now().UTC()
	lower := strings.ToLower(key)
	one := func(p *string) ([]string, bool, error) {
		if p == nil {
			return nil, false, nil
		}
		return []string{*p}, true, nil
	}
	switch lower {
	case "aws:currenttime":
		return []string{now.Format(time.RFC3339)}, true, nil
	case "aws:epochtime":
		return []string{strconv.FormatInt(now.Unix(), 10)}, true, nil
	case "aws:principalaccount":
		return one(r.Authorization.AccountId)
	case "aws:resourceaccount":
		return one(r.ResourceAccountId)
	case "aws:sourceip":
		if r.HttpRequest.ClientIP != nil {
			return one(r.HttpRequest.ClientIP)
		}
		return one(r.HttpRequest.RemoteIP)
	case "aws:securetransport":
		return []string{strconv.FormatBool(strings.EqualFold(r.HttpRequest.Scheme, "https"))}, true, nil
	case "aws:useragent":
		return header(r, "User-Agent")
	case "aws:referer":
		return header(r, "Referer")
	case "pithos:principalid":
		return one(r.Authorization.PrincipalId)
	case "pithos:accesskeyid":
		return one(r.Authorization.AccessKeyId)
	case "pithos:authtype", "s3:authtype":
		if r.Authorization.AccessKeyId == nil {
			return []string{"Anonymous"}, true, nil
		}
		if r.Authorization.AuthType == "" {
			return nil, false, nil
		}
		return []string{r.Authorization.AuthType}, true, nil
	case "s3:versionid":
		return one(r.VersionID)
	case "s3:prefix":
		return query(r, "prefix")
	case "s3:delimiter":
		return query(r, "delimiter")
	case "s3:max-keys":
		return query(r, "max-keys")
	case "s3:signatureversion":
		if r.Authorization.AccessKeyId != nil {
			return []string{"AWS4-HMAC-SHA256"}, true, nil
		}
		return nil, false, nil
	case "s3:requestobjecttagkeys":
		v := make([]string, 0, len(r.RequestObjectTags))
		for k := range r.RequestObjectTags {
			v = append(v, k)
		}
		return v, len(v) > 0, nil
	case "s3:object-lock-mode":
		return one(r.ObjectLockMode)
	case "s3:object-lock-retain-until-date":
		return one(r.ObjectLockRetainUntilDate)
	case "s3:object-lock-legal-hold":
		return one(r.ObjectLockLegalHold)
	case "s3:object-lock-remaining-retention-days":
		if r.ObjectLockDays != nil {
			return []string{strconv.FormatInt(int64(*r.ObjectLockDays), 10)}, true, nil
		}
		if r.ObjectLockYears != nil {
			return []string{strconv.FormatInt(int64(*r.ObjectLockYears)*365, 10)}, true, nil
		}
		return nil, false, nil
	}
	if strings.HasPrefix(lower, "s3:requestobjecttag/") {
		v, ok := r.RequestObjectTags[key[len("s3:RequestObjectTag/"):]]
		return []string{v}, ok, nil
	}
	if strings.HasPrefix(lower, "s3:existingobjecttag/") {
		if !*resolved {
			resolver := r.ResolveExistingObjectTags
			if source {
				resolver = r.ResolveExistingSourceObjectTags
			}
			if resolver == nil {
				return nil, false, nil
			}
			v, err := resolver(ctx)
			if err != nil {
				return nil, false, fmt.Errorf("resolve existing object tags: %w", err)
			}
			*tags = v
			*resolved = true
		}
		v, ok := (*tags)[key[len("s3:ExistingObjectTag/"):]]
		return []string{v}, ok, nil
	}
	return nil, false, nil
}
func header(r *authorization.Request, k string) ([]string, bool, error) {
	for n, v := range r.HttpRequest.Headers {
		if strings.EqualFold(n, k) {
			return v, len(v) > 0, nil
		}
	}
	return nil, false, nil
}
func query(r *authorization.Request, k string) ([]string, bool, error) {
	v, ok := r.HttpRequest.QueryParams[k]
	return v, ok, nil
}

func compareCondition(operator string, actual, expected []string, present bool) (bool, error) {
	parts, err := parseConditionOperator(operator)
	if err != nil {
		return false, err
	}
	setAll := parts.setAll
	setAny := parts.setAny
	op := parts.base
	if op == "Null" {
		want, err := strconv.ParseBool(expected[0])
		return want == !present, err
	}
	negative := strings.Contains(op, "Not") || op == "NotIpAddress"
	if !present {
		if setAll {
			return true, nil
		}
		return negative, nil
	}
	positiveOp := strings.ReplaceAll(op, "Not", "")
	matchOne := func(a, e string) (bool, error) { return scalarCompare(positiveOp, a, e) }
	matchActual := func(a string) (bool, error) {
		matched := false
		for _, e := range expected {
			ok, err := matchOne(a, e)
			if err != nil {
				return false, err
			}
			if ok {
				matched = true
				break
			}
		}
		if negative {
			return !matched, nil
		}
		return matched, nil
	}
	if setAll {
		for _, a := range actual {
			ok, err := matchActual(a)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	if setAny || len(actual) > 0 {
		for _, a := range actual {
			ok, err := matchActual(a)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
	}
	return false, nil
}
func scalarCompare(op, a, e string) (bool, error) {
	base := op
	var ok bool
	switch {
	case strings.HasPrefix(base, "String"), strings.HasPrefix(base, "Arn"):
		if strings.Contains(base, "IgnoreCase") {
			ok = strings.EqualFold(a, e)
		} else if strings.Contains(base, "Like") {
			ok = wildcard(e, a, false)
		} else {
			ok = a == e
		}
	case strings.HasPrefix(base, "Numeric"):
		av, err := strconv.ParseFloat(a, 64)
		if err != nil {
			return false, err
		}
		ev, err := strconv.ParseFloat(e, 64)
		if err != nil {
			return false, err
		}
		ok = ordered(base, av, ev)
	case strings.HasPrefix(base, "Date"):
		av, err := time.Parse(time.RFC3339, a)
		if err != nil {
			return false, err
		}
		ev, err := time.Parse(time.RFC3339, e)
		if err != nil {
			return false, err
		}
		ok = ordered(base, float64(av.UnixNano()), float64(ev.UnixNano()))
	case base == "Bool":
		av, err := strconv.ParseBool(a)
		if err != nil {
			return false, err
		}
		ev, err := strconv.ParseBool(e)
		if err != nil {
			return false, err
		}
		ok = av == ev
	case base == "IpAddress":
		ip := net.ParseIP(a)
		if expectedIP := net.ParseIP(e); expectedIP != nil {
			ok = ip != nil && ip.Equal(expectedIP)
		} else {
			_, cidr, err := net.ParseCIDR(e)
			if err != nil {
				return false, err
			}
			ok = ip != nil && cidr.Contains(ip)
		}
	default:
		return false, fmt.Errorf("unsupported condition operator %s", op)
	}
	return ok, nil
}
func ordered(op string, a, b float64) bool {
	switch {
	case strings.HasSuffix(op, "LessThan"):
		return a < b
	case strings.HasSuffix(op, "LessThanEquals"):
		return a <= b
	case strings.HasSuffix(op, "GreaterThan"):
		return a > b
	case strings.HasSuffix(op, "GreaterThanEquals"):
		return a >= b
	default:
		return a == b
	}
}

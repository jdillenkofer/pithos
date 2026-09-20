package policy

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

var allowedConditionKeys = []string{
	"aws:CurrentTime", "aws:EpochTime", "aws:PrincipalAccount", "aws:ResourceAccount", "aws:SourceIp", "aws:SecureTransport", "aws:UserAgent", "aws:Referer",
	"pithos:PrincipalId", "pithos:AccessKeyId", "pithos:AuthType", "s3:prefix", "s3:delimiter", "s3:max-keys", "s3:VersionId", "s3:authType", "s3:signatureversion",
	"s3:RequestObjectTagKeys", "s3:object-lock-mode", "s3:object-lock-retain-until-date", "s3:object-lock-legal-hold", "s3:object-lock-remaining-retention-days",
}

var baseOperators = map[string]bool{
	"StringEquals": true, "StringNotEquals": true, "StringEqualsIgnoreCase": true, "StringNotEqualsIgnoreCase": true, "StringLike": true, "StringNotLike": true,
	"ArnEquals": true, "ArnLike": true, "ArnNotEquals": true, "ArnNotLike": true,
	"NumericEquals": true, "NumericNotEquals": true, "NumericLessThan": true, "NumericLessThanEquals": true, "NumericGreaterThan": true, "NumericGreaterThanEquals": true,
	"DateEquals": true, "DateNotEquals": true, "DateLessThan": true, "DateLessThanEquals": true, "DateGreaterThan": true, "DateGreaterThanEquals": true,
	"Bool": true, "IpAddress": true, "NotIpAddress": true, "Null": true,
}

func Load(path string) (*Snapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() > maxPolicyFileSize {
		return nil, fmt.Errorf("policy file exceeds 1 MiB")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Compile(data)
}

func Compile(data []byte) (*Snapshot, error) {
	if len(data) > maxPolicyFileSize {
		return nil, fmt.Errorf("policy file exceeds 1 MiB")
	}
	var f File
	if err := strictDecode(data, &f); err != nil {
		return nil, fmt.Errorf("$: %w", err)
	}
	if f.SchemaVersion != 1 {
		return nil, fmt.Errorf("$.schemaVersion: expected 1")
	}
	if len(f.Policies) == 0 {
		return nil, fmt.Errorf("$.policies: must not be empty")
	}
	compiled := map[string][]compiledStatement{}
	for name, doc := range f.Policies {
		if doc.Version != "2012-10-17" {
			return nil, fmt.Errorf("$.policies.%s.Version: unsupported version", name)
		}
		var statements []Statement
		if err := strictDecode(doc.Statement, &statements); err != nil {
			var one Statement
			if err2 := strictDecode(doc.Statement, &one); err2 != nil {
				return nil, fmt.Errorf("$.policies.%s.Statement: %w", name, err)
			}
			statements = []Statement{one}
		}
		if len(statements) == 0 {
			return nil, fmt.Errorf("$.policies.%s.Statement: must not be empty", name)
		}
		for i, s := range statements {
			cs, err := compileStatement(name, s)
			if err != nil {
				return nil, fmt.Errorf("$.policies.%s.Statement[%d]: %w", name, i, err)
			}
			compiled[name] = append(compiled[name], cs)
		}
	}
	s := &Snapshot{bySubject: map[string][]compiledStatement{}}
	for i, b := range f.Bindings {
		statements, ok := compiled[b.Policy]
		if !ok {
			return nil, fmt.Errorf("$.bindings[%d].policy: unknown policy %q", i, b.Policy)
		}
		if len(b.Subjects) == 0 {
			return nil, fmt.Errorf("$.bindings[%d].subjects: must not be empty", i)
		}
		for j, subject := range b.Subjects {
			key, err := subjectKey(subject)
			if err != nil {
				return nil, fmt.Errorf("$.bindings[%d].subjects[%d]: %w", i, j, err)
			}
			s.bySubject[key] = append(s.bySubject[key], statements...)
		}
	}
	return s, nil
}

func subjectKey(s Subject) (string, error) {
	switch s.Type {
	case "anonymous":
		if s.AccountID != "" || s.PrincipalID != "" {
			return "", fmt.Errorf("anonymous subject has principal fields")
		}
		return "anonymous", nil
	case "principal":
		if s.AccountID == "" || s.PrincipalID == "" {
			return "", fmt.Errorf("principal requires accountId and principalId")
		}
		return "principal\x00" + s.AccountID + "\x00" + s.PrincipalID, nil
	default:
		return "", fmt.Errorf("unsupported type %q", s.Type)
	}
}

func compileStatement(policy string, s Statement) (compiledStatement, error) {
	if s.Effect != "Allow" && s.Effect != "Deny" {
		return compiledStatement{}, fmt.Errorf("Effect must be Allow or Deny")
	}
	if len(s.Action) == 0 || len(s.Resource) == 0 {
		return compiledStatement{}, fmt.Errorf("Action and Resource must not be empty")
	}
	for _, a := range s.Action {
		valid := false
		for _, known := range SupportedActions() {
			if wildcard(a, known) {
				valid = true
				break
			}
		}
		if !valid {
			return compiledStatement{}, fmt.Errorf("Action: unsupported action %q", a)
		}
	}
	for _, r := range s.Resource {
		if r != "*" && !strings.HasPrefix(r, "arn:aws:s3:::") {
			return compiledStatement{}, fmt.Errorf("Resource: invalid S3 ARN %q", r)
		}
	}
	cs := compiledStatement{policy: policy, sid: s.Sid, effect: s.Effect, actions: s.Action, resources: s.Resource}
	for op, entries := range s.Condition {
		base := strings.TrimSuffix(op, "IfExists")
		base = strings.TrimPrefix(base, "ForAnyValue:")
		base = strings.TrimPrefix(base, "ForAllValues:")
		if !baseOperators[base] {
			return compiledStatement{}, fmt.Errorf("Condition.%s: unsupported operator", op)
		}
		for key, raw := range entries {
			if !validConditionKey(key) {
				return compiledStatement{}, fmt.Errorf("Condition.%s.%s: unsupported key", op, key)
			}
			var vals stringList
			if err := json.Unmarshal(raw, &vals); err != nil {
				return compiledStatement{}, fmt.Errorf("Condition.%s.%s: %w", op, key, err)
			}
			if len(vals) == 0 {
				return compiledStatement{}, fmt.Errorf("Condition.%s.%s: empty values", op, key)
			}
			cs.conditions = append(cs.conditions, condition{op, key, vals})
		}
	}
	return cs, nil
}
func validConditionKey(k string) bool {
	for _, v := range allowedConditionKeys {
		if strings.EqualFold(k, v) {
			return true
		}
	}
	return strings.HasPrefix(strings.ToLower(k), "s3:existingobjecttag/") || strings.HasPrefix(strings.ToLower(k), "s3:requestobjecttag/")
}

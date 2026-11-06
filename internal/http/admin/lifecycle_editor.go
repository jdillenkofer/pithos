package admin

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
)

type lifecycleTransitionForm struct{ Class, Days, Date, Newer string }

func lifecyclePredicates(rule storage.LifecycleRule) (string, []storage.LifecycleTag, *int64, *int64) {
	var prefix string
	var tags []storage.LifecycleTag
	var greater, less *int64
	if rule.Prefix != nil {
		prefix = *rule.Prefix
	}
	if f := rule.Filter; f != nil {
		if f.Prefix != nil {
			prefix = *f.Prefix
		}
		if f.Tag != nil {
			tags = append(tags, *f.Tag)
		}
		greater, less = f.ObjectSizeGreaterThan, f.ObjectSizeLessThan
		if a := f.And; a != nil {
			if a.Prefix != nil {
				prefix = *a.Prefix
			}
			tags = append(tags, a.Tags...)
			greater, less = a.ObjectSizeGreaterThan, a.ObjectSizeLessThan
		}
	}
	return prefix, tags, greater, less
}

func lifecycleInt64(text, label string, min int64) (*int64, error) {
	if text == "" {
		return nil, nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < min {
		return nil, fmt.Errorf("%s must be an integer of at least %d", label, min)
	}
	return &n, nil
}

func lifecycleDate(text string) (*time.Time, error) {
	if text == "" {
		return nil, nil
	}
	date, err := time.Parse("2006-01-02", text)
	if err != nil {
		return nil, errors.New("enter a valid date (YYYY-MM-DD); lifecycle dates use midnight UTC")
	}
	return &date, nil
}

// lifecycleRuleFromFields parses the advanced controls. The basic day-based
// actions are parsed by lifecycleFromForm before validating the complete rule.
func lifecycleRuleFromFields(form url.Values) (storage.LifecycleRule, error) {
	rule := storage.LifecycleRule{}
	prefix := form.Get("prefix")
	greater, err := lifecycleInt64(form.Get("size_greater"), "Minimum size (exclusive)", 0)
	if err != nil {
		return rule, err
	}
	less, err := lifecycleInt64(form.Get("size_less"), "Maximum size (exclusive)", 1)
	if err != nil {
		return rule, err
	}
	keys, values := form["filter_tag_key"], form["filter_tag_value"]
	if len(keys) != len(values) {
		return rule, errors.New("each filter tag requires a key and value")
	}
	var tags []storage.LifecycleTag
	for i, key := range keys {
		if key == "" && values[i] == "" {
			continue
		}
		if key == "" {
			return rule, errors.New("enter a key for each filter tag")
		}
		tags = append(tags, storage.LifecycleTag{Key: key, Value: values[i]})
	}
	// Normalize combined predicates into And; a lone prefix stays a simple filter.
	rule.Filter = &storage.LifecycleFilter{Prefix: &prefix}
	if len(tags) > 0 || greater != nil || less != nil {
		rule.Filter = &storage.LifecycleFilter{And: &storage.LifecycleFilterAnd{Tags: tags, ObjectSizeGreaterThan: greater, ObjectSizeLessThan: less}}
		if prefix != "" {
			rule.Filter.And.Prefix = &prefix
		}
	}
	date, err := lifecycleDate(form.Get("expire_date"))
	if err != nil {
		return rule, err
	}
	if date != nil {
		if form.Get("expire_days") != "" {
			return rule, errors.New("choose days or a date for expiration, not both")
		}
		rule.Expiration = &storage.LifecycleExpiration{Date: date}
	}
	if marker := form.Get("expired_delete_markers"); marker != "" {
		if marker != "true" && marker != "false" {
			return rule, errors.New("invalid delete marker action")
		}
		if date != nil || form.Get("expire_days") != "" {
			return rule, errors.New("delete marker cleanup cannot be combined with current version expiration")
		}
		enabled := marker == "true"
		rule.Expiration = &storage.LifecycleExpiration{ExpiredObjectDeleteMarker: &enabled}
	}
	if form.Get("newer_noncurrent") != "" && form.Get("noncurrent_days") == "" {
		return rule, errors.New("set noncurrent expiration days when retaining newer versions")
	}
	for _, noncurrent := range []bool{false, true} {
		field := "transition_"
		if noncurrent {
			field = "noncurrent_transition_"
		}
		classes, days, dates, newer := form[field+"class"], form[field+"days"], form[field+"date"], form[field+"newer"]
		if len(classes) != len(days) || (!noncurrent && len(classes) != len(dates)) || (noncurrent && len(classes) != len(newer)) {
			return rule, errors.New("incomplete transition row")
		}
		if len(classes) > 100 {
			return rule, errors.New("too many transitions")
		}
		for i, class := range classes {
			dateText, newerText := "", ""
			if noncurrent {
				newerText = newer[i]
			} else {
				dateText = dates[i]
			}
			if class == "" && days[i] == "" && dateText == "" && newerText == "" {
				continue
			}
			n, err := lifecycleInt64(days[i], "Transition days", 0)
			if err != nil {
				return rule, err
			}
			var count *int32
			if n != nil {
				if *n > 2147483647 {
					return rule, errors.New("transition days are too large")
				}
				value := int32(*n)
				count = &value
			}
			if noncurrent {
				retain, err := positiveInt32(url.Values{"retain": {newerText}}, "retain", "Newer versions to retain")
				if err != nil {
					return rule, err
				}
				rule.NoncurrentVersionTransitions = append(rule.NoncurrentVersionTransitions, storage.LifecycleNoncurrentVersionTransition{StorageClass: class, NoncurrentDays: count, NewerNoncurrentVersions: retain})
			} else {
				date, err := lifecycleDate(dateText)
				if err != nil {
					return rule, err
				}
				rule.Transitions = append(rule.Transitions, storage.LifecycleTransition{StorageClass: class, Days: count, Date: date})
			}
		}
	}
	return rule, nil
}

func lifecycleFormForRule(rule storage.LifecycleRule) lifecycleFormView {
	form := lifecycleFormView{Status: rule.Status}
	if rule.ID != nil {
		form.ID, form.OriginalID = *rule.ID, *rule.ID
	}
	prefix, tags, greater, less := lifecyclePredicates(rule)
	form.Prefix = prefix
	for _, tag := range tags {
		form.Tags = append(form.Tags, keyValue{tag.Key, tag.Value})
	}
	if greater != nil {
		form.SizeGreater = strconv.FormatInt(*greater, 10)
	}
	if less != nil {
		form.SizeLess = strconv.FormatInt(*less, 10)
	}
	if e := rule.Expiration; e != nil {
		form.ExpireDays = int32Text(e.Days)
		if e.Date != nil {
			form.ExpireDate = e.Date.UTC().Format("2006-01-02")
		}
		if e.ExpiredObjectDeleteMarker != nil {
			form.MarkerMode = strconv.FormatBool(*e.ExpiredObjectDeleteMarker)
		}
	}
	if e := rule.NoncurrentVersionExpiration; e != nil {
		form.NoncurrentDays, form.NewerNoncurrent = int32Text(e.NoncurrentDays), int32Text(e.NewerNoncurrentVersions)
	}
	if e := rule.AbortIncompleteMultipartUpload; e != nil {
		form.Abort = int32Text(e.DaysAfterInitiation)
	}
	for _, t := range rule.Transitions {
		row := lifecycleTransitionForm{Class: t.StorageClass, Days: int32Text(t.Days)}
		if t.Date != nil {
			row.Date = t.Date.UTC().Format("2006-01-02")
		}
		form.Transitions = append(form.Transitions, row)
	}
	for _, t := range rule.NoncurrentVersionTransitions {
		form.NoncurrentTransitions = append(form.NoncurrentTransitions, lifecycleTransitionForm{Class: t.StorageClass, Days: int32Text(t.NoncurrentDays), Newer: int32Text(t.NewerNoncurrentVersions)})
	}
	return form
}

func lifecycleTransitionRows(rows []lifecycleTransitionForm) []lifecycleTransitionForm {
	return append(rows, lifecycleTransitionForm{})
}

// lifecycleRuleSummary exposes every predicate and action without JSON.
func lifecycleRuleSummary(rule storage.LifecycleRule) lifecycleView {
	view := lifecycleView{ID: "Unnamed rule", Status: rule.Status, Current: "-", Noncurrent: "-", Other: "-"}
	if rule.ID != nil && *rule.ID != "" {
		view.ID = *rule.ID
	}
	prefix, tags, greater, less := lifecyclePredicates(rule)
	var scope, current, noncurrent, other []string
	if prefix != "" {
		scope = append(scope, "Prefix "+prefix)
	}
	for _, tag := range tags {
		scope = append(scope, "Tag "+tag.Key+" = "+tag.Value)
	}
	if greater != nil {
		scope = append(scope, fmt.Sprintf("Size > %d bytes", *greater))
	}
	if less != nil {
		scope = append(scope, fmt.Sprintf("Size < %d bytes", *less))
	}
	view.Scope = "Entire bucket"
	if len(scope) > 0 {
		view.Scope = strings.Join(scope, "; ")
	}
	if e := rule.Expiration; e != nil {
		if e.Days != nil {
			current = append(current, "Expire after "+int32Text(e.Days)+" days")
		}
		if e.Date != nil {
			current = append(current, "Expire on "+e.Date.UTC().Format("2006-01-02"))
		}
		if e.ExpiredObjectDeleteMarker != nil {
			other = append(other, "Expired delete marker cleanup: "+strconv.FormatBool(*e.ExpiredObjectDeleteMarker))
		}
	}
	for _, t := range rule.Transitions {
		when := "after " + int32Text(t.Days) + " days"
		if t.Date != nil {
			when = "on " + t.Date.UTC().Format("2006-01-02")
		}
		current = append(current, "Move to "+t.StorageClass+" "+when)
	}
	if e := rule.NoncurrentVersionExpiration; e != nil {
		noncurrent = append(noncurrent, "Delete after "+int32Text(e.NoncurrentDays)+" noncurrent days"+lifecycleRetainSummary(e.NewerNoncurrentVersions))
	}
	for _, t := range rule.NoncurrentVersionTransitions {
		noncurrent = append(noncurrent, "Move to "+t.StorageClass+" after "+int32Text(t.NoncurrentDays)+" noncurrent days"+lifecycleRetainSummary(t.NewerNoncurrentVersions))
	}
	if e := rule.AbortIncompleteMultipartUpload; e != nil {
		other = append(other, "Abort incomplete uploads after "+int32Text(e.DaysAfterInitiation)+" days")
	}
	if len(current) > 0 {
		view.Current = strings.Join(current, "; ")
	}
	if len(noncurrent) > 0 {
		view.Noncurrent = strings.Join(noncurrent, "; ")
	}
	if len(other) > 0 {
		view.Other = strings.Join(other, "; ")
	}
	return view
}

func lifecycleRetainSummary(count *int32) string {
	if count == nil {
		return ""
	}
	return ", keep " + int32Text(count) + " newer versions"
}

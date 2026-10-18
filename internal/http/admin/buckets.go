package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/middleware"
	s3 "github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage"
)

const maxBucketFormSize = 1 << 20

var bucketTabs = []string{"versioning", "object-lock", "lifecycle", "cors", "website", "tags", "notifications"}

type bucketRow struct {
	Name, Owner, Created, Objects, Size string
}

type bucketDeleteView struct {
	CSRF, Name, Owner string
}

type bucketsPage struct {
	CSRF, Owner, Query string
	Total              int
	Buckets            []bucketRow
}

// bucketHeader drives the shared bucket title and S3-console-style tabs.
type bucketHeader struct {
	Name, Owner, Active string
}

type bucketDetailPage struct {
	CSRF    string
	Bucket  *storage.Bucket
	Header  bucketHeader
	Tabs    []string
	Tab     string
	Created string

	// Properties tab
	Versioning string
	ObjectLock objectLockView
	Tags       []keyValue
	Website    websiteView

	WebsiteHash          string
	WebsiteRouting       []websiteRoutingView
	WebsiteRoutingRule   websiteRoutingFormView
	WebsiteRoutingEditor bool

	Notifications            []notificationView
	NotificationRule         notificationFormView
	NotificationEventGroups  []notificationEventGroup
	NotificationHash         string
	NotificationEditor       bool
	NotificationAdvancedOpen bool
	EventBridgeEnabled       bool

	// Permissions and Management tabs
	CORS            []corsView
	CORSRule        corsFormView
	CORSMethods     []string
	CORSHash        string
	CORSEditor      bool
	Lifecycle       []lifecycleView
	LifecycleRule   lifecycleFormView
	LifecycleEditor bool
}

// bucketTabGroups places each configuration on the S3 console tab where it lives.
var bucketTabGroups = map[string]string{
	"versioning": "properties", "object-lock": "properties", "tags": "properties", "website": "properties", "notifications": "properties",
	"cors": "permissions", "lifecycle": "management",
}

func bucketGroupTabs(group string) []string {
	var tabs []string
	for _, tab := range bucketTabs {
		if bucketTabGroups[tab] == group {
			tabs = append(tabs, tab)
		}
	}
	return tabs
}

// registerBuckets must be mounted behind the global-admin session and CSRF
// middleware. Storage calls deliberately bypass S3 ownership and policy checks.
func (a *App) registerBuckets(mux *http.ServeMux) {
	mux.HandleFunc("GET /buckets", a.listBuckets)
	mux.HandleFunc("GET /buckets/new", a.newBucket)
	mux.HandleFunc("POST /buckets/create", a.createBucket)
	mux.HandleFunc("GET /buckets/delete", a.confirmDeleteBucket)
	mux.HandleFunc("POST /buckets/delete", a.deleteBucket)
	mux.HandleFunc("GET /buckets/detail", a.bucketDetail)
	mux.HandleFunc("POST /buckets/configure", a.configureBucket)
	a.registerEmptyBucket(mux)
}

func (a *App) bucketError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	message := "Bucket operation failed"
	switch {
	case errors.Is(err, storage.ErrNoSuchBucket):
		status, message = http.StatusNotFound, "Bucket not found"
	case errors.Is(err, storage.ErrBucketAlreadyExists):
		status, message = http.StatusConflict, "Bucket already exists"
	case errors.Is(err, storage.ErrBucketNotEmpty):
		status, message = http.StatusConflict, "Bucket is not empty"
	case errors.Is(err, storage.ErrObjectLockConfigurationNotFound), errors.Is(err, storage.ErrInvalidObjectLockConfiguration), errors.Is(err, storage.ErrObjectLockAccessDenied):
		status, message = http.StatusConflict, "Object lock configuration prevents this operation"
	}
	a.fail(w, r, status, message)
}

func (a *App) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := a.options.Storage.ListBuckets(r.Context())
	if err != nil {
		a.bucketError(w, r, err)
		return
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name.String() < buckets[j].Name.String() })
	page := bucketsPage{CSRF: csrfToken(r), Owner: currentCredential(r).AccountID, Query: r.URL.Query().Get("q"), Total: len(buckets)}
	// Usage comes from the periodic bucket gauges; unsampled buckets show a dash.
	snapshot, _ := gatherMetrics()
	for _, bucket := range buckets {
		if page.Query != "" && !strings.Contains(bucket.Name.String(), strings.ToLower(page.Query)) {
			continue
		}
		row := bucketRow{Name: bucket.Name.String(), Owner: bucket.OwnerAccountID, Objects: "—", Size: "—"}
		if !bucket.CreationDate.IsZero() {
			row.Created = bucket.CreationDate.UTC().Format("2006-01-02 15:04") + " UTC"
		}
		if usage, ok := snapshot.buckets[row.Name]; ok {
			row.Objects, row.Size = formatCount(usage.Objects), humanBytes(usage.Bytes)
		}
		page.Buckets = append(page.Buckets, row)
	}
	a.render(w, r, "Buckets", "buckets", page)
}

func (a *App) newBucket(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, "Create bucket", "buckets_new", bucketsPage{CSRF: csrfToken(r), Owner: currentCredential(r).AccountID})
}

func (a *App) bucketForm(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBucketFormSize)
	if err := r.ParseForm(); err != nil {
		a.fail(w, r, http.StatusBadRequest, "Invalid or oversized bucket form")
		return false
	}
	return true
}

func (a *App) createBucket(w http.ResponseWriter, r *http.Request) {
	if !a.bucketForm(w, r) {
		return
	}
	name, err := storage.NewBucketName(r.PostForm.Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket name")
		return
	}
	owner := r.PostForm.Get("owner")
	if owner == "" {
		owner = currentCredential(r).AccountID
	}
	if strings.TrimSpace(owner) == "" || len(owner) > authentication.MaxAccountIDLength {
		a.fail(w, r, 400, "Invalid owner account")
		return
	}
	lock := r.PostForm.Get("object_lock")
	if lock != "" && lock != "true" && lock != "false" {
		a.fail(w, r, 400, "Invalid object lock value")
		return
	}
	versioning := r.PostForm.Get("versioning")
	if versioning != "" && versioning != "Enabled" && versioning != "Disabled" {
		a.fail(w, r, 400, "Invalid versioning value")
		return
	}
	if err := a.options.Storage.CreateBucket(r.Context(), name, storage.CreateBucketOptions{OwnerAccountID: owner, ObjectLockEnabled: lock == "true"}); err != nil {
		a.bucketError(w, r, err)
		return
	}
	a.audit(r, "AdminCreateBucket")
	// Object Lock already enables versioning on creation.
	if versioning == "Enabled" && lock != "true" {
		status := storage.BucketVersioningStatusEnabled
		if err := a.options.Storage.PutBucketVersioningConfiguration(r.Context(), name, &storage.BucketVersioningConfiguration{Status: &status}); err != nil {
			a.fail(w, r, http.StatusInternalServerError, "The bucket was created, but enabling versioning failed. Enable it in the bucket properties.")
			return
		}
		a.audit(r, "AdminPutBucketVersioning")
	}
	http.Redirect(w, r, "/objects?bucket="+url.QueryEscape(name.String()), http.StatusSeeOther)
}

// confirmDeleteBucket shows the S3-console-style deletion page for the
// bucket selected in the bucket list. The POST still checks the typed name.
func (a *App) confirmDeleteBucket(w http.ResponseWriter, r *http.Request) {
	name, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Select a bucket to delete")
		return
	}
	bucket, err := a.options.Storage.HeadBucket(r.Context(), name)
	if err != nil {
		a.bucketError(w, r, err)
		return
	}
	a.render(w, r, "Delete bucket", "buckets_delete", bucketDeleteView{CSRF: csrfToken(r), Name: name.String(), Owner: bucket.OwnerAccountID})
}

func (a *App) deleteBucket(w http.ResponseWriter, r *http.Request) {
	if !a.bucketForm(w, r) {
		return
	}
	name, err := storage.NewBucketName(r.PostForm.Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket name")
		return
	}
	if r.PostForm.Get("confirm") != name.String() {
		a.fail(w, r, 400, "Type the bucket name to confirm deletion")
		return
	}
	if err := a.options.Storage.DeleteBucket(r.Context(), name); err != nil {
		a.bucketError(w, r, err)
		return
	}
	a.audit(r, "AdminDeleteBucket")
	http.Redirect(w, r, "/buckets", http.StatusSeeOther)
}

func validBucketTab(tab string) bool {
	for _, candidate := range bucketTabs {
		if tab == candidate {
			return true
		}
	}
	return false
}

func (a *App) bucketDetail(w http.ResponseWriter, r *http.Request) {
	name, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket name")
		return
	}
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "versioning"
	}
	if !validBucketTab(tab) {
		a.fail(w, r, 400, "Unknown bucket configuration")
		return
	}
	bucket, err := a.options.Storage.HeadBucket(r.Context(), name)
	if err != nil {
		a.bucketError(w, r, err)
		return
	}
	group := bucketTabGroups[tab]
	page := bucketDetailPage{CSRF: csrfToken(r), Bucket: bucket, Header: bucketHeader{Name: name.String(), Owner: bucket.OwnerAccountID, Active: group}, Tabs: bucketGroupTabs(group), Tab: tab}
	if !bucket.CreationDate.IsZero() {
		page.Created = bucket.CreationDate.UTC().Format("2006-01-02 15:04") + " UTC"
	}
	ctx := r.Context()
	load := func(get func() (any, error)) (any, bool) {
		config, err := get()
		if err != nil && !missingBucketConfiguration(err) {
			a.bucketError(w, r, err)
			return nil, false
		}
		if err != nil {
			return nil, true
		}
		return config, true
	}
	switch group {
	case "properties":
		versioning, ok := load(func() (any, error) { return a.options.Storage.GetBucketVersioningConfiguration(ctx, name) })
		if !ok {
			return
		}
		page.Versioning = "Disabled"
		if c, _ := versioning.(*storage.BucketVersioningConfiguration); c != nil && c.Status != nil {
			page.Versioning = string(*c.Status)
		}
		lock, ok := load(func() (any, error) { return a.options.Storage.GetObjectLockConfiguration(ctx, name) })
		if !ok {
			return
		}
		lockConfig, _ := lock.(*storage.ObjectLockConfiguration)
		page.ObjectLock = describeObjectLock(lockConfig)
		tags, ok := load(func() (any, error) { return a.options.Storage.GetBucketTagging(ctx, name) })
		if !ok {
			return
		}
		tagMap, _ := tags.(map[string]string)
		page.Tags = sortedTags(tagMap)
		website, ok := load(func() (any, error) { return a.options.Storage.GetBucketWebsiteConfiguration(ctx, name) })
		if !ok {
			return
		}
		websiteConfig, _ := website.(*storage.WebsiteConfiguration)
		page.Website = describeWebsite(websiteConfig)
		if websiteConfig == nil {
			websiteConfig = &storage.WebsiteConfiguration{}
		}
		page.WebsiteHash = policyHash([]byte(configurationText(websiteConfig)))
		page.WebsiteRouting = describeWebsiteRouting(websiteConfig)
		page.WebsiteRoutingRule = websiteRoutingRuleForm(storage.WebsiteRoutingRule{}, "", len(websiteConfig.RoutingRules))
		if tab == "website" {
			query := r.URL.Query()
			page.WebsiteRoutingEditor = query.Get("editor") == "create" || query.Has("rule_index")
			if page.WebsiteRoutingEditor && (!page.Website.Enabled || page.Website.Redirect) {
				a.fail(w, r, http.StatusBadRequest, "Enable static website hosting before editing redirection rules")
				return
			}
			if query.Has("rule_index") {
				index, err := strconv.Atoi(query.Get("rule_index"))
				if err != nil || index < 0 || index >= len(websiteConfig.RoutingRules) {
					a.fail(w, r, http.StatusNotFound, "Redirection rule not found")
					return
				}
				page.WebsiteRoutingRule = websiteRoutingRuleForm(websiteConfig.RoutingRules[index], strconv.Itoa(index), len(websiteConfig.RoutingRules))
			}
		}
		notifications, ok := load(func() (any, error) { return a.options.Storage.GetBucketNotificationConfiguration(ctx, name) })
		if !ok {
			return
		}
		notificationConfig, _ := notifications.(*storage.BucketNotificationConfiguration)
		if notificationConfig == nil {
			notificationConfig = &storage.BucketNotificationConfiguration{}
		}
		page.Notifications = describeNotifications(notificationConfig)
		page.EventBridgeEnabled = notificationConfig.EventBridgeEnabled
		page.NotificationHash = policyHash([]byte(configurationText(notificationConfig)))
		page.NotificationRule = notificationRuleForm(storage.NotificationConfigurationRule{}, "", "")
		if tab == "notifications" {
			query := r.URL.Query()
			page.NotificationEditor = query.Get("editor") == "create" || query.Has("rule_index") || query.Has("rule_type")
			if query.Has("rule_index") || query.Has("rule_type") {
				rules := notificationRules(notificationConfig, query.Get("rule_type"))
				index, err := strconv.Atoi(query.Get("rule_index"))
				if err != nil || rules == nil || index < 0 || index >= len(*rules) {
					a.fail(w, r, http.StatusNotFound, "Notification rule not found")
					return
				}
				page.NotificationRule = notificationRuleForm((*rules)[index], query.Get("rule_type"), strconv.Itoa(index))
			}
		}
		page.NotificationEventGroups = notificationGroups(page.NotificationRule.Events)
		for _, group := range page.NotificationEventGroups {
			if group.Advanced {
				for _, event := range group.Events {
					page.NotificationAdvancedOpen = page.NotificationAdvancedOpen || page.NotificationRule.Events[event]
				}
			}
		}
	case "permissions":
		cors, ok := load(func() (any, error) { return a.options.Storage.GetBucketCORSConfiguration(ctx, name) })
		if !ok {
			return
		}
		corsConfig, _ := cors.(*storage.BucketCORSConfiguration)
		if corsConfig == nil {
			corsConfig = &storage.BucketCORSConfiguration{}
		}
		page.CORS = describeCORS(corsConfig)
		page.CORSHash = policyHash([]byte(configurationText(corsConfig)))
		page.CORSMethods = corsMethods
		page.CORSEditor = r.URL.Query().Get("editor") == "create" || r.URL.Query().Has("rule_index")
		page.CORSRule = corsRuleForm(storage.CORSRule{}, "")
		if r.URL.Query().Has("rule_index") {
			index, err := strconv.Atoi(r.URL.Query().Get("rule_index"))
			if err != nil || index < 0 || index >= len(corsConfig.Rules) {
				a.fail(w, r, http.StatusNotFound, "CORS rule not found")
				return
			}
			page.CORSRule = corsRuleForm(corsConfig.Rules[index], strconv.Itoa(index))
		}
	case "management":
		lifecycle, ok := load(func() (any, error) { return a.options.Storage.GetBucketLifecycleConfiguration(ctx, name) })
		if !ok {
			return
		}
		lifecycleConfig, _ := lifecycle.(*storage.BucketLifecycleConfiguration)
		page.Lifecycle = describeLifecycle(name.String(), lifecycleConfig)
		page.LifecycleRule = lifecycleRuleForm(lifecycleConfig, r.URL.Query().Get("rule"))
		page.LifecycleEditor = r.URL.Query().Get("editor") == "create" || r.URL.Query().Has("rule") || r.URL.Query().Has("rule_index")
		if r.URL.Query().Has("rule_index") {
			index, err := strconv.Atoi(r.URL.Query().Get("rule_index"))
			if err != nil || lifecycleConfig == nil || index < 0 || index >= len(lifecycleConfig.Rules) {
				a.fail(w, r, http.StatusNotFound, "Lifecycle rule not found")
				return
			}
			page.LifecycleRule = lifecycleFormForRule(lifecycleConfig.Rules[index])
			page.LifecycleRule.OriginalIndex = strconv.Itoa(index)
		}
		page.LifecycleRule.Tags = append(page.LifecycleRule.Tags, keyValue{})
		page.LifecycleRule.Transitions = lifecycleTransitionRows(page.LifecycleRule.Transitions)
		page.LifecycleRule.NoncurrentTransitions = lifecycleTransitionRows(page.LifecycleRule.NoncurrentTransitions)
		if r.URL.Query().Has("rule") && page.LifecycleRule.OriginalID == "" {
			a.fail(w, r, http.StatusNotFound, "Lifecycle rule not found")
			return
		}
	}
	a.render(w, r, "Bucket: "+name.String(), "buckets_detail", page)
}

// configurationText formats configurations for snapshot hashes and typed validation.
func configurationText(config any) string {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil || string(data) == "null" {
		return "{}"
	}
	return string(data)
}

func missingBucketConfiguration(err error) bool {
	return errors.Is(err, storage.ErrNoSuchLifecycleConfiguration) || errors.Is(err, storage.ErrNoSuchCORSConfiguration) || errors.Is(err, storage.ErrNoSuchWebsiteConfiguration) || errors.Is(err, storage.ErrObjectLockConfigurationNotFound)
}

// decodeBucketConfiguration accepts exactly one non-null typed JSON value.
func decodeBucketConfiguration(text string, target any) error {
	if len(text) > maxBucketFormSize || strings.TrimSpace(text) == "null" {
		return fmt.Errorf("configuration must be a JSON object")
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("configuration must contain exactly one JSON value")
	}
	return nil
}

func (a *App) configureBucket(w http.ResponseWriter, r *http.Request) {
	if !a.bucketForm(w, r) {
		return
	}
	name, err := storage.NewBucketName(r.PostForm.Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket name")
		return
	}
	tab := r.PostForm.Get("tab")
	if !validBucketTab(tab) {
		a.fail(w, r, 400, "Unknown bucket configuration")
		return
	}
	text, action := r.PostForm.Get("configuration"), r.PostForm.Get("action")
	if r.PostForm.Get("editor") == "form" {
		text, action, err = structuredBucketConfiguration(r.Context(), a.options.Storage, name, tab, r.PostForm)
		if err != nil {
			a.fail(w, r, 400, err.Error())
			return
		}
	}
	// Validate before invoking any storage mutation.
	apply, err := bucketConfigurationMutation(a.options.Storage, name, tab, text, action)
	if err == nil {
		if tab == "website" && r.PostForm.Get("editor") == "form" && r.PostForm.Get("website_rule_action") == "save" {
			if !a.confirmed(w, r, websiteRoutingSaveConfirmation(name.String(), text, r.PostForm)) {
				return
			}
		}
		if tab == "lifecycle" && r.PostForm.Get("editor") == "form" && r.PostForm.Get("rule_action") != "delete" {
			if !a.confirmed(w, r, lifecycleSaveConfirmation(name.String(), text, r.PostForm)) {
				return
			}
		}
		if c, destructive := bucketDeletionConfirmation(name.String(), tab, action, r.PostForm); destructive && !a.confirmed(w, r, c) {
			return
		}
	}
	if err != nil {
		a.fail(w, r, 400, err.Error())
		return
	}
	if err := apply(r); err != nil {
		a.bucketError(w, r, err)
		return
	}
	a.audit(r, bucketConfigurationEvent(tab, action))
	http.Redirect(w, r, "/buckets/detail?bucket="+url.QueryEscape(name.String())+"&tab="+url.QueryEscape(tab)+"#"+tab, http.StatusSeeOther)
}

// bucketDeletionConfirmation describes configuration removals, including
// individual rule deletions, which need explicit confirmation.
func bucketDeletionConfirmation(bucket, tab, action string, form url.Values) (confirmation, bool) {
	if tab == "website" {
		switch form.Get("website_rule_action") {
		case "delete":
			return confirmation{Title: "Delete website redirection rule?", Message: "The redirection rule is removed from bucket " + bucket + ". Other rules and website hosting settings are kept.", Action: "Delete rule"}, true
		case "delete_all":
			return confirmation{Title: "Delete all website redirection rules?", Message: "All redirection rules are removed from bucket " + bucket + ". Website hosting remains enabled and its index and error documents are kept.", Action: "Delete all rules"}, true
		}
	}
	if tab == "notifications" {
		switch form.Get("notification_action") {
		case "delete":
			return confirmation{Title: "Delete event notification?", Message: "This rule is removed from bucket " + bucket + ". Events will no longer be sent through this rule. Other rules and EventBridge are kept.", Action: "Delete notification"}, true
		case "delete_all":
			return confirmation{Title: "Delete all event notification rules?", Message: "All event notification rules are removed from bucket " + bucket + ". The EventBridge setting is kept.", Action: "Delete all rules"}, true
		}
	}
	if tab == "cors" && form.Get("cors_action") == "delete" {
		return confirmation{Title: "Delete CORS rule?", Message: "This rule is removed from bucket " + bucket + ". Browser applications relying on it may lose cross-origin access.", Action: "Delete rule"}, true
	}
	if form.Get("rule_action") == "delete" {
		rule := form.Get("original_id")
		return confirmation{Title: "Delete lifecycle rule " + rule + "?", Message: "The rule is removed from bucket " + bucket + " and its actions stop applying.", Action: "Delete rule"}, true
	}
	if action != "delete" {
		return confirmation{}, false
	}
	names := map[string]string{"lifecycle": "lifecycle configuration", "cors": "CORS configuration", "website": "static website hosting configuration", "tags": "tags"}
	return confirmation{
		Title:   "Delete the " + names[tab] + " of " + bucket + "?",
		Message: "The " + names[tab] + " of bucket " + bucket + " is removed. This cannot be undone.",
		Action:  "Delete",
	}, true
}

// bucketConfigurationEvent names the audit event like the S3 operation, for
// example AdminPutBucketLifecycle or AdminDeleteBucketCors.
func bucketConfigurationEvent(tab, action string) string {
	names := map[string]string{
		"versioning": "BucketVersioning", "object-lock": "ObjectLockConfiguration", "lifecycle": "BucketLifecycle",
		"cors": "BucketCors", "website": "BucketWebsite", "tags": "BucketTagging", "notifications": "BucketNotification",
	}
	if action == "delete" {
		return "AdminDelete" + names[tab]
	}
	return "AdminPut" + names[tab]
}

func bucketConfigurationMutation(st storage.Storage, name storage.BucketName, tab, text, action string) (func(*http.Request) error, error) {
	if action == "delete" {
		switch tab {
		case "lifecycle":
			return func(r *http.Request) error { return st.DeleteBucketLifecycleConfiguration(r.Context(), name) }, nil
		case "cors":
			return func(r *http.Request) error { return st.DeleteBucketCORSConfiguration(r.Context(), name) }, nil
		case "website":
			return func(r *http.Request) error { return st.DeleteBucketWebsiteConfiguration(r.Context(), name) }, nil
		case "tags":
			return func(r *http.Request) error { return st.DeleteBucketTagging(r.Context(), name) }, nil
		default:
			return nil, fmt.Errorf("this configuration cannot be deleted; save an empty notifications configuration or suspend versioning instead")
		}
	}
	if action != "" && action != "save" {
		return nil, fmt.Errorf("unknown configuration action")
	}
	switch tab {
	case "versioning":
		var c storage.BucketVersioningConfiguration
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		if c.Status == nil || (*c.Status != storage.BucketVersioningStatusEnabled && *c.Status != storage.BucketVersioningStatusSuspended) {
			return nil, fmt.Errorf("versioning Status must be Enabled or Suspended")
		}
		return func(r *http.Request) error { return st.PutBucketVersioningConfiguration(r.Context(), name, &c) }, nil
	case "object-lock":
		var c storage.ObjectLockConfiguration
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return func(r *http.Request) error { return st.PutObjectLockConfiguration(r.Context(), name, &c) }, nil
	case "lifecycle":
		var c storage.BucketLifecycleConfiguration
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		if err := storage.ValidateBucketLifecycleConfiguration(&c); err != nil {
			return nil, err
		}
		return func(r *http.Request) error { return st.PutBucketLifecycleConfiguration(r.Context(), name, &c) }, nil
	case "cors":
		var c storage.BucketCORSConfiguration
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		rules, err := middleware.NormalizeAndValidateCORSRules(c.Rules)
		if err != nil {
			return nil, err
		}
		c.Rules = rules
		return func(r *http.Request) error { return st.PutBucketCORSConfiguration(r.Context(), name, &c) }, nil
	case "website":
		var c storage.WebsiteConfiguration
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		config, err := s3.ValidateWebsiteConfiguration(&c)
		if err != nil {
			return nil, err
		}
		return func(r *http.Request) error { return st.PutBucketWebsiteConfiguration(r.Context(), name, config) }, nil
	case "tags":
		var c map[string]string
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		if err := storage.ValidateBucketTags(c); err != nil {
			return nil, err
		}
		return func(r *http.Request) error { return st.PutBucketTagging(r.Context(), name, c) }, nil
	case "notifications":
		var c storage.BucketNotificationConfiguration
		if err := decodeBucketConfiguration(text, &c); err != nil {
			return nil, err
		}
		config, err := s3.ValidateNotificationConfiguration(&c)
		if err != nil {
			return nil, err
		}
		return func(r *http.Request) error { return st.PutBucketNotificationConfiguration(r.Context(), name, config) }, nil
	}
	return nil, fmt.Errorf("unknown bucket configuration")
}

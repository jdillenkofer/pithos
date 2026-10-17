package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/middleware"
	s3 "github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage"
)

const maxBucketFormSize = 1 << 20

var bucketTabs = []string{"versioning", "object-lock", "lifecycle", "cors", "website", "tags", "notifications"}

type bucketsPage struct {
	CSRF    string
	Owner   string
	Buckets []storage.Bucket
}

type bucketDetailPage struct {
	CSRF          string
	Bucket        *storage.Bucket
	Tabs          []string
	Tab           string
	Configuration string
}

// registerBuckets must be mounted behind the global-admin session and CSRF
// middleware. Storage calls deliberately bypass S3 ownership and policy checks.
func (a *App) registerBuckets(mux *http.ServeMux) {
	mux.HandleFunc("GET /buckets", a.listBuckets)
	mux.HandleFunc("POST /buckets/create", a.createBucket)
	mux.HandleFunc("POST /buckets/delete", a.deleteBucket)
	mux.HandleFunc("GET /buckets/detail", a.bucketDetail)
	mux.HandleFunc("POST /buckets/configure", a.configureBucket)
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
	a.render(w, r, "Buckets", "buckets", bucketsPage{CSRF: csrfToken(r), Owner: currentCredential(r).AccountID, Buckets: buckets})
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
	if err := a.options.Storage.CreateBucket(r.Context(), name, storage.CreateBucketOptions{OwnerAccountID: owner, ObjectLockEnabled: lock == "true"}); err != nil {
		a.bucketError(w, r, err)
		return
	}
	a.audit(r, "CreateBucket")
	http.Redirect(w, r, "/buckets/detail?bucket="+url.QueryEscape(name.String()), http.StatusSeeOther)
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
	a.audit(r, "DeleteBucket")
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
	var config any
	switch tab {
	case "versioning":
		config, err = a.options.Storage.GetBucketVersioningConfiguration(r.Context(), name)
	case "object-lock":
		config, err = a.options.Storage.GetObjectLockConfiguration(r.Context(), name)
	case "lifecycle":
		config, err = a.options.Storage.GetBucketLifecycleConfiguration(r.Context(), name)
	case "cors":
		config, err = a.options.Storage.GetBucketCORSConfiguration(r.Context(), name)
	case "website":
		config, err = a.options.Storage.GetBucketWebsiteConfiguration(r.Context(), name)
	case "tags":
		config, err = a.options.Storage.GetBucketTagging(r.Context(), name)
	case "notifications":
		config, err = a.options.Storage.GetBucketNotificationConfiguration(r.Context(), name)
	}
	if err != nil && !missingBucketConfiguration(err) {
		a.bucketError(w, r, err)
		return
	}
	text := "{}"
	if err == nil && config != nil {
		data, marshalErr := json.MarshalIndent(config, "", "  ")
		if marshalErr != nil {
			a.bucketError(w, r, marshalErr)
			return
		}
		if string(data) != "null" {
			text = string(data)
		}
	}
	a.render(w, r, "Bucket: "+name.String(), "buckets_detail", bucketDetailPage{CSRF: csrfToken(r), Bucket: bucket, Tabs: bucketTabs, Tab: tab, Configuration: text})
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
	// Validate before invoking any storage mutation.
	apply, err := bucketConfigurationMutation(a.options.Storage, name, tab, r.PostForm.Get("configuration"), r.PostForm.Get("action"))
	if err != nil {
		a.fail(w, r, 400, err.Error())
		return
	}
	if err := apply(r); err != nil {
		a.bucketError(w, r, err)
		return
	}
	a.audit(r, "ConfigureBucket:"+tab)
	http.Redirect(w, r, "/buckets/detail?bucket="+url.QueryEscape(name.String())+"&tab="+url.QueryEscape(tab), http.StatusSeeOther)
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

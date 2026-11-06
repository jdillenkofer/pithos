package admin

import (
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage"
)

// maxListedVersions bounds the versions page and one batch deletion.
const maxListedVersions = 1000

// permanentDeleteConfirmation must be typed to delete versions, as in the S3
// console, because version deletion cannot be undone.
const permanentDeleteConfirmation = "permanently delete"

type versionRow struct {
	Version, Modified, SizeText, StorageClass string
	DetailURL, ViewURL, DownloadURL           string
	Latest, DeleteMarker                      bool
}

type objectVersionsView struct {
	CSRF, Bucket, Key, Name, BucketURL, DetailURL, VersionsURL string
	Crumbs                                                     []objectLink
	Versions                                                   []versionRow
	Truncated                                                  bool
}

type deleteVersionsView struct {
	objectVersionsView
	Selected []string
	Results  []deleteVersionResult
	Deleted  int
}

type deleteVersionResult struct {
	Version, Error string
}

func (a *App) registerVersions(mux *http.ServeMux) {
	mux.HandleFunc("GET /objects/versions", a.objectVersions)
	mux.HandleFunc("GET /objects/delete-versions", a.confirmDeleteVersions)
	mux.HandleFunc("POST /objects/delete-versions", a.deleteVersions)
}

func (a *App) versionsBase(r *http.Request, b storage.BucketName, k storage.ObjectKey) objectVersionsView {
	key := k.String()
	return objectVersionsView{
		CSRF: csrfToken(r), Bucket: b.String(), Key: key, Name: path.Base(key),
		BucketURL:   "/objects?" + url.Values{"bucket": {b.String()}}.Encode(),
		DetailURL:   objectURL("/objects/detail", b.String(), key, ""),
		VersionsURL: objectURL("/objects/versions", b.String(), key, ""),
		Crumbs:      prefixCrumbs(b.String(), key[:strings.LastIndex(key, "/")+1], false),
	}
}

// listKeyVersions returns the versions of exactly one key, newest first.
func (a *App) listKeyVersions(r *http.Request, b storage.BucketName, k storage.ObjectKey) ([]storage.ObjectVersion, bool, error) {
	key := k.String()
	opts := storage.ListObjectVersionsOptions{Prefix: &key, MaxKeys: maxListedVersions}
	var versions []storage.ObjectVersion
	for {
		result, err := a.options.Storage.ListObjectVersions(r.Context(), b, opts)
		if err != nil {
			return nil, false, err
		}
		for _, version := range result.Versions {
			// Listings are ordered by key, so longer keys sharing the prefix come after.
			if version.Key.String() != key {
				return versions, false, nil
			}
			if len(versions) == maxListedVersions {
				return versions, true, nil
			}
			versions = append(versions, version)
		}
		if !result.IsTruncated || result.NextKeyMarker == nil {
			return versions, false, nil
		}
		opts.KeyMarker, opts.VersionIDMarker = result.NextKeyMarker, result.NextVersionIDMarker
	}
}

func (a *App) objectVersions(w http.ResponseWriter, r *http.Request) {
	b, k, _, err := objectTarget(r)
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	versions, truncated, err := a.listKeyVersions(r, b, k)
	if err != nil {
		a.objectError(w, r, err)
		return
	}
	view := a.versionsBase(r, b, k)
	view.Truncated = truncated
	for _, version := range versions {
		row := versionRow{
			Version: version.VersionID, Modified: formatModified(version.LastModified), Latest: version.IsLatest, DeleteMarker: version.IsDeleteMarker,
			SizeText: humanBytes(float64(version.Size)), StorageClass: storageClassLabel(version.StorageClass),
			DetailURL: objectURL("/objects/detail", b.String(), k.String(), version.VersionID),
		}
		if !version.IsDeleteMarker {
			row.DownloadURL = objectURL("/objects/download", b.String(), k.String(), version.VersionID)
			row.ViewURL = listViewURL(b.String(), k.String(), version.VersionID, false)
		}
		view.Versions = append(view.Versions, row)
	}
	a.render(w, r, "Object details", "object-versions", view)
}

// selectedVersions returns the distinct, non-empty version IDs in the form.
func selectedVersions(form url.Values) ([]string, bool) {
	seen := map[string]bool{}
	var versions []string
	for _, version := range form["version"] {
		if version == "" || seen[version] {
			continue
		}
		if len(versions) == maxListedVersions || len(version) > 1024 {
			return nil, false
		}
		seen[version] = true
		versions = append(versions, version)
	}
	return versions, len(versions) > 0
}

func (a *App) confirmDeleteVersions(w http.ResponseWriter, r *http.Request) {
	b, err := storage.NewBucketName(r.FormValue("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	k, err := storage.NewObjectKey(r.FormValue("key"))
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	versions, ok := selectedVersions(r.URL.Query())
	if !ok {
		a.fail(w, r, 400, "Select at least one version to delete")
		return
	}
	a.render(w, r, "Delete versions", "delete-versions", deleteVersionsView{objectVersionsView: a.versionsBase(r, b, k), Selected: versions})
}

func (a *App) deleteVersions(w http.ResponseWriter, r *http.Request) {
	b, err := storage.NewBucketName(r.FormValue("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	k, err := storage.NewObjectKey(r.FormValue("key"))
	if err != nil {
		a.fail(w, r, 400, "Invalid object target")
		return
	}
	versions, ok := selectedVersions(r.PostForm)
	if !ok {
		a.fail(w, r, 400, "Select at least one version to delete")
		return
	}
	if strings.TrimSpace(r.PostForm.Get("confirm_text")) != permanentDeleteConfirmation {
		a.fail(w, r, 400, "Type \""+permanentDeleteConfirmation+"\" to confirm")
		return
	}
	// Only allowlisted global admins reach this route, including the explicit
	// governance bypass capability. Compliance retention is never bypassed.
	bypass := r.PostForm.Get("bypass_governance") == "yes"
	view := deleteVersionsView{objectVersionsView: a.versionsBase(r, b, k), Selected: versions}
	refs := make([]objectRef, len(versions))
	for i, version := range versions {
		refs[i] = objectRef{Key: k.String(), Version: version}
	}
	failures := a.deleteSelected(r.Context(), b, refs, bypass)
	for i, version := range versions {
		view.Results = append(view.Results, deleteVersionResult{Version: version, Error: failures[i]})
		if failures[i] == "" {
			view.Deleted++
		}
	}
	a.audit(r, "AdminDeleteObjectVersions")
	if bypass {
		a.audit(r, "AdminBypassGovernanceRetention")
	}
	a.render(w, r, "Delete versions", "delete-versions", view)
}

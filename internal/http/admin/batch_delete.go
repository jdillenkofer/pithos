package admin

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage"
)

// deleteObjectsConfirmation and its permanent variant must be typed, as in
// the S3 console. Deleting a specific version cannot be undone.
const deleteObjectsConfirmation = "delete"

// objectRef selects one object or, with a version, one object version.
type objectRef struct {
	Key     string `json:"k"`
	Version string `json:"v,omitempty"`
}

// Value encodes the reference as a single form value.
func (o objectRef) Value() string {
	data, _ := json.Marshal(o)
	return string(data)
}

type deleteObjectsView struct {
	CSRF, Bucket, BackURL, Confirmation string
	Objects                             []objectRef
	Permanent                           bool
	Results                             []deleteObjectResult
	Deleted                             int
}

type deleteObjectResult struct {
	objectRef
	Error string
}

func (a *App) registerBatchDelete(mux *http.ServeMux) {
	mux.HandleFunc("GET /objects/delete-objects", a.confirmDeleteObjects)
	mux.HandleFunc("POST /objects/delete-objects", a.deleteObjects)
}

// selectedObjects decodes the distinct objects selected in a form.
func selectedObjects(form url.Values) ([]objectRef, bool) {
	seen := map[objectRef]bool{}
	var objects []objectRef
	for _, value := range form["object"] {
		var ref objectRef
		if len(value) > 4096 || json.Unmarshal([]byte(value), &ref) != nil {
			return nil, false
		}
		if _, err := storage.NewObjectKey(ref.Key); err != nil || len(ref.Version) > 1024 {
			return nil, false
		}
		if seen[ref] {
			continue
		}
		if len(objects) == maxListedVersions {
			return nil, false
		}
		seen[ref] = true
		objects = append(objects, ref)
	}
	return objects, len(objects) > 0
}

func newDeleteObjectsView(r *http.Request, bucket string, objects []objectRef) deleteObjectsView {
	view := deleteObjectsView{CSRF: csrfToken(r), Bucket: bucket, Objects: objects, Confirmation: deleteObjectsConfirmation}
	for _, object := range objects {
		if object.Version != "" {
			view.Permanent, view.Confirmation = true, permanentDeleteConfirmation
		}
	}
	view.BackURL = sameOriginBackLink(r)
	if !strings.HasPrefix(view.BackURL, "/objects?") {
		view.BackURL = "/objects?" + url.Values{"bucket": {bucket}}.Encode()
	}
	return view
}

func (a *App) confirmDeleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket")
		return
	}
	objects, ok := selectedObjects(r.URL.Query())
	if !ok {
		a.fail(w, r, 400, "Select at least one object to delete")
		return
	}
	a.render(w, r, "Delete objects", "delete-objects", newDeleteObjectsView(r, bucket.String(), objects))
}

func (a *App) deleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket, err := storage.NewBucketName(r.PostForm.Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket")
		return
	}
	objects, ok := selectedObjects(r.PostForm)
	if !ok {
		a.fail(w, r, 400, "Select at least one object to delete")
		return
	}
	view := newDeleteObjectsView(r, bucket.String(), objects)
	if back := r.PostForm.Get("back"); strings.HasPrefix(back, "/objects?") {
		view.BackURL = back
	}
	if strings.TrimSpace(r.PostForm.Get("confirm_text")) != view.Confirmation {
		a.fail(w, r, 400, "Type \""+view.Confirmation+"\" to confirm")
		return
	}
	// Only allowlisted global admins reach this route, including the explicit
	// governance bypass capability. Compliance retention is never bypassed.
	bypass := r.PostForm.Get("bypass_governance") == "yes"
	for _, object := range objects {
		result := deleteObjectResult{objectRef: object}
		opts := &storage.DeleteObjectOptions{BypassGovernanceRetention: bypass}
		if object.Version != "" {
			version := object.Version
			opts.VersionID = &version
		}
		_, err := a.options.Storage.DeleteObject(r.Context(), bucket, storage.MustNewObjectKey(object.Key), opts)
		if err != nil {
			_, result.Error = objectErrorMessage(err)
		} else {
			view.Deleted++
			if object.Version != "" {
				a.audit(r, "AdminDeleteObjectVersion")
			} else {
				a.audit(r, "AdminDeleteObject")
			}
		}
		view.Results = append(view.Results, result)
	}
	if bypass {
		a.audit(r, "AdminBypassGovernanceRetention")
	}
	a.render(w, r, "Delete objects", "delete-objects", view)
}

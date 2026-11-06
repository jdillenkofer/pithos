package admin

import (
	"net/http"
	"sort"
)

// confirmation describes an action that needs explicit consent or review.
type confirmation struct {
	Title, Message, Action string
	Review                 bool
	Details                []keyValue
	Warning                string
}

type confirmView struct {
	confirmation
	CSRF, Target, CancelURL string
	Fields                  []keyValue
}

// confirmed reports whether the request carries the confirmation of a
// destructive action. Otherwise it answers with a page that re-submits the
// same form with confirm=yes, so deletions never depend on JavaScript.
// Handlers must return when confirmed is false.
func (a *App) confirmed(w http.ResponseWriter, r *http.Request, c confirmation) bool {
	if r.PostForm.Get("confirm") == "yes" {
		return true
	}
	view := confirmView{confirmation: c, CSRF: csrfToken(r), Target: r.URL.Path, CancelURL: sameOriginBackLink(r)}
	if view.CancelURL == "" {
		view.CancelURL = "/"
	}
	names := make([]string, 0, len(r.PostForm))
	for name := range r.PostForm {
		if name != "csrf_token" && name != "confirm" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		for _, value := range r.PostForm[name] {
			view.Fields = append(view.Fields, keyValue{name, value})
		}
	}
	a.render(w, r, c.Title, "confirm", view)
	return false
}

package admin

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

// lifecycleHTMLForm models browser submission, excluding inert templates.
func lifecycleHTMLForm(t *testing.T, page string) url.Values {
	t.Helper()
	doc, err := html.Parse(strings.NewReader(page))
	require.NoError(t, err)
	values := url.Values{}
	attr := func(n *html.Node, key string) string {
		for _, a := range n.Attr {
			if a.Key == key {
				return a.Val
			}
		}
		return ""
	}
	var visit func(*html.Node)
	visit = func(n *html.Node) {
		if n.Type == html.ElementNode {
			if n.Data == "template" {
				return
			}
			if n.Data == "input" && attr(n, "name") != "" {
				values.Add(attr(n, "name"), attr(n, "value"))
			}
			if n.Data == "select" {
				var chosen string
				first := true
				for option := n.FirstChild; option != nil; option = option.NextSibling {
					if option.Type != html.ElementNode || option.Data != "option" {
						continue
					}
					value, explicit := "", false
					selected := false
					for _, a := range option.Attr {
						if a.Key == "value" {
							value, explicit = a.Val, true
						}
						if a.Key == "selected" {
							selected = true
						}
					}
					if !explicit && option.FirstChild != nil {
						value = option.FirstChild.Data
					}
					if first || selected {
						chosen = value
						first = false
					}
				}
				values.Add(attr(n, "name"), chosen)
				return
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(doc)
	return values
}

func TestLifecycleRenderedEditorRoundTripsAllActions(t *testing.T) {
	id, prefix := "advanced", "reports/"
	days, newer, zero, size := int32(90), int32(5), int32(0), int64(0)
	date, err := lifecycleDate("2031-01-01")
	require.NoError(t, err)
	rule := storage.LifecycleRule{ID: &id, Status: "Disabled",
		Filter:                       &storage.LifecycleFilter{And: &storage.LifecycleFilterAnd{Prefix: &prefix, Tags: []storage.LifecycleTag{{Key: "team", Value: "storage"}, {Key: "empty", Value: ""}}, ObjectSizeGreaterThan: &size}},
		Expiration:                   &storage.LifecycleExpiration{Date: date},
		Transitions:                  []storage.LifecycleTransition{{Days: &zero, StorageClass: "STANDARD_IA"}, {Date: date, StorageClass: "GLACIER"}},
		NoncurrentVersionExpiration:  &storage.LifecycleNoncurrentVersionExpiration{NoncurrentDays: &days, NewerNoncurrentVersions: &newer},
		NoncurrentVersionTransitions: []storage.LifecycleNoncurrentVersionTransition{{NoncurrentDays: &newer, NewerNoncurrentVersions: &newer, StorageClass: "DEEP_ARCHIVE"}},
	}
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}, lifecycle: &storage.BucketLifecycleConfiguration{Rules: []storage.LifecycleRule{rule}}}
	a := bucketTestApp(t, s)
	page := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle&rule=advanced", nil)
	require.Equal(t, http.StatusOK, page.Code, page.Body.String())
	form := lifecycleHTMLForm(t, page.Body.String())
	form.Set("confirm", "yes")
	result := bucketRequest(a, "POST", "/buckets/configure", form)
	require.Equal(t, http.StatusSeeOther, result.Code, result.Body.String())
	require.Equal(t, rule, s.last.(*storage.BucketLifecycleConfiguration).Rules[0])
}

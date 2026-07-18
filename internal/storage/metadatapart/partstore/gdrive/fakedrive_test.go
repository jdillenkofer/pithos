package gdrive

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// fakeDriveServer is a minimal in-memory implementation of the subset of the
// Google Drive v3 REST API used by gdrivePartStore: files.list with a small
// query language, files.create (metadata-only and multipart media upload),
// files.get (metadata and alt=media), files.update (rename) and files.delete.
type fakeDriveServer struct {
	mu     sync.Mutex
	files  map[string]*fakeDriveFile
	nextId int
	server *httptest.Server
}

type fakeDriveFile struct {
	Id       string
	Name     string
	MimeType string
	Parents  []string
	Content  []byte
	Trashed  bool
	Seq      int
}

func newFakeDriveServer() *fakeDriveServer {
	f := &fakeDriveServer{
		files: map[string]*fakeDriveFile{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /files", f.handleList)
	mux.HandleFunc("POST /files", f.handleCreateMetadata)
	mux.HandleFunc("POST /upload/drive/v3/files", f.handleUpload)
	mux.HandleFunc("PATCH /upload/drive/v3/files/{id}", f.handleUploadUpdate)
	mux.HandleFunc("GET /files/{id}", f.handleGet)
	mux.HandleFunc("PATCH /files/{id}", f.handlePatch)
	mux.HandleFunc("DELETE /files/{id}", f.handleDelete)
	f.server = httptest.NewServer(mux)
	return f
}

func (f *fakeDriveServer) URL() string {
	return f.server.URL + "/"
}

func (f *fakeDriveServer) Close() {
	f.server.Close()
}

// addFile injects a file directly into the fake store, bypassing the API.
func (f *fakeDriveServer) addFile(name string, mimeType string, parents []string, content []byte) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.insertLocked(name, mimeType, parents, content)
}

func (f *fakeDriveServer) insertLocked(name string, mimeType string, parents []string, content []byte) string {
	f.nextId++
	id := fmt.Sprintf("file-%d", f.nextId)
	f.files[id] = &fakeDriveFile{
		Id:       id,
		Name:     name,
		MimeType: mimeType,
		Parents:  parents,
		Content:  content,
		Seq:      f.nextId,
	}
	return id
}

func (f *fakeDriveServer) fileCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.files)
}

func writeJson(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeApiError(w http.ResponseWriter, status int, message string) {
	writeJson(w, status, map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
		},
	})
}

func fileResource(file *fakeDriveFile) map[string]any {
	return map[string]any{
		"id":       file.Id,
		"name":     file.Name,
		"mimeType": file.MimeType,
		"parents":  file.Parents,
		"size":     strconv.FormatInt(int64(len(file.Content)), 10),
	}
}

// driveQuery is the parsed form of the query expressions the part store
// issues: conjunctions of name = '…', mimeType = '…', '…' in parents and
// trashed = false.
type driveQuery struct {
	name     *string
	mimeType *string
	parent   *string
}

func unescapeQueryValue(value string) string {
	var unescaped strings.Builder
	escaped := false
	for _, r := range value {
		if !escaped && r == '\\' {
			escaped = true
			continue
		}
		escaped = false
		unescaped.WriteRune(r)
	}
	return unescaped.String()
}

func parseDriveQuery(q string) (*driveQuery, error) {
	query := &driveQuery{}
	if q == "" {
		return query, nil
	}
	for clause := range strings.SplitSeq(q, " and ") {
		clause = strings.TrimSpace(clause)
		switch {
		case clause == "trashed = false":
			// Trashed files are always excluded below.
		case strings.HasPrefix(clause, "name = '") && strings.HasSuffix(clause, "'"):
			name := unescapeQueryValue(strings.TrimSuffix(strings.TrimPrefix(clause, "name = '"), "'"))
			query.name = &name
		case strings.HasPrefix(clause, "mimeType = '") && strings.HasSuffix(clause, "'"):
			mimeType := unescapeQueryValue(strings.TrimSuffix(strings.TrimPrefix(clause, "mimeType = '"), "'"))
			query.mimeType = &mimeType
		case strings.HasPrefix(clause, "'") && strings.HasSuffix(clause, "' in parents"):
			parent := unescapeQueryValue(strings.TrimSuffix(strings.TrimPrefix(clause, "'"), "' in parents"))
			query.parent = &parent
		default:
			return nil, fmt.Errorf("unsupported query clause: %q", clause)
		}
	}
	return query, nil
}

func (f *fakeDriveServer) handleList(w http.ResponseWriter, r *http.Request) {
	query, err := parseDriveQuery(r.URL.Query().Get("q"))
	if err != nil {
		writeApiError(w, http.StatusBadRequest, err.Error())
		return
	}

	f.mu.Lock()
	matches := []*fakeDriveFile{}
	for _, file := range f.files {
		if file.Trashed {
			continue
		}
		if query.name != nil && file.Name != *query.name {
			continue
		}
		if query.mimeType != nil && file.MimeType != *query.mimeType {
			continue
		}
		if query.parent != nil && !slices.Contains(file.Parents, *query.parent) {
			continue
		}
		matches = append(matches, file)
	}
	f.mu.Unlock()

	orderBy := r.URL.Query().Get("orderBy")
	sort.Slice(matches, func(i, j int) bool {
		if orderBy == "createdTime desc" {
			return matches[i].Seq > matches[j].Seq
		}
		return matches[i].Seq < matches[j].Seq
	})
	if pageSizeStr := r.URL.Query().Get("pageSize"); pageSizeStr != "" {
		pageSize, err := strconv.Atoi(pageSizeStr)
		if err == nil && len(matches) > pageSize {
			matches = matches[:pageSize]
		}
	}

	fileResources := []map[string]any{}
	for _, file := range matches {
		fileResources = append(fileResources, fileResource(file))
	}
	writeJson(w, http.StatusOK, map[string]any{
		"files":         fileResources,
		"nextPageToken": "",
	})
}

func (f *fakeDriveServer) handleCreateMetadata(w http.ResponseWriter, r *http.Request) {
	var metadata fakeDriveFileMetadata
	if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
		writeApiError(w, http.StatusBadRequest, err.Error())
		return
	}
	f.mu.Lock()
	id := f.insertLocked(metadata.Name, metadata.MimeType, metadata.Parents, nil)
	file := f.files[id]
	f.mu.Unlock()
	writeJson(w, http.StatusOK, fileResource(file))
}

func (f *fakeDriveServer) handleUploadUpdate(w http.ResponseWriter, r *http.Request) {
	_, content, err := parseMultipartUploadBody(r)
	if err != nil {
		writeApiError(w, http.StatusBadRequest, err.Error())
		return
	}
	f.mu.Lock()
	file, ok := f.files[r.PathValue("id")]
	if ok {
		file.Content = content
	}
	f.mu.Unlock()
	if !ok {
		writeApiError(w, http.StatusNotFound, "File not found")
		return
	}
	writeJson(w, http.StatusOK, fileResource(file))
}

type fakeDriveFileMetadata struct {
	Name     string   `json:"name"`
	MimeType string   `json:"mimeType"`
	Parents  []string `json:"parents"`
}

func parseMultipartUploadBody(r *http.Request) (fakeDriveFileMetadata, []byte, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return fakeDriveFileMetadata{}, nil, fmt.Errorf("expected multipart/related body")
	}
	multipartReader := multipart.NewReader(r.Body, params["boundary"])

	metadataPart, err := multipartReader.NextPart()
	if err != nil {
		return fakeDriveFileMetadata{}, nil, err
	}
	var metadata fakeDriveFileMetadata
	if err := json.NewDecoder(metadataPart).Decode(&metadata); err != nil {
		return fakeDriveFileMetadata{}, nil, err
	}

	contentPart, err := multipartReader.NextPart()
	if err != nil {
		return fakeDriveFileMetadata{}, nil, err
	}
	content, err := io.ReadAll(contentPart)
	if err != nil {
		return fakeDriveFileMetadata{}, nil, err
	}
	return metadata, content, nil
}

func (f *fakeDriveServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if uploadType := r.URL.Query().Get("uploadType"); uploadType != "multipart" {
		writeApiError(w, http.StatusBadRequest, fmt.Sprintf("fake server only supports uploadType=multipart, got %q", uploadType))
		return
	}
	metadata, content, err := parseMultipartUploadBody(r)
	if err != nil {
		writeApiError(w, http.StatusBadRequest, err.Error())
		return
	}

	f.mu.Lock()
	id := f.insertLocked(metadata.Name, metadata.MimeType, metadata.Parents, content)
	file := f.files[id]
	f.mu.Unlock()
	writeJson(w, http.StatusOK, fileResource(file))
}

func (f *fakeDriveServer) handleGet(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	file, ok := f.files[r.PathValue("id")]
	f.mu.Unlock()
	if !ok || file.Trashed {
		writeApiError(w, http.StatusNotFound, "File not found")
		return
	}
	if r.URL.Query().Get("alt") == "media" {
		content := file.Content
		status := http.StatusOK
		if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
			offsetStr, ok := strings.CutPrefix(rangeHeader, "bytes=")
			offsetStr, _, _ = strings.Cut(offsetStr, "-")
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if !ok || err != nil {
				writeApiError(w, http.StatusBadRequest, "unsupported range header")
				return
			}
			if offset >= int64(len(content)) {
				writeApiError(w, http.StatusRequestedRangeNotSatisfiable, "range not satisfiable")
				return
			}
			content = content[offset:]
			status = http.StatusPartialContent
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(status)
		_, _ = w.Write(content)
		return
	}
	writeJson(w, http.StatusOK, fileResource(file))
}

func (f *fakeDriveServer) handlePatch(w http.ResponseWriter, r *http.Request) {
	var metadata fakeDriveFileMetadata
	if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
		writeApiError(w, http.StatusBadRequest, err.Error())
		return
	}
	f.mu.Lock()
	file, ok := f.files[r.PathValue("id")]
	if ok && metadata.Name != "" {
		file.Name = metadata.Name
	}
	f.mu.Unlock()
	if !ok {
		writeApiError(w, http.StatusNotFound, "File not found")
		return
	}
	writeJson(w, http.StatusOK, fileResource(file))
}

func (f *fakeDriveServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	_, ok := f.files[r.PathValue("id")]
	delete(f.files, r.PathValue("id"))
	f.mu.Unlock()
	if !ok {
		writeApiError(w, http.StatusNotFound, "File not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

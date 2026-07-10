package server

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	httpmiddleware "github.com/jdillenkofer/pithos/internal/http/middleware"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/corscache"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Server struct {
	requestAuthorizer authorization.RequestAuthorizer
	storage           storage.Storage
	tracer            trace.Tracer
}

func SetupServer(credentials []settings.Credentials, region string, apiEndpoint string, websiteEndpoint string, requestAuthorizer authorization.RequestAuthorizer, storage storage.Storage) http.Handler {
	server := &Server{
		requestAuthorizer: requestAuthorizer,
		// CORS configuration is resolved on every Origin-bearing request, so wrap
		// the storage in a cache that serves those reads without re-hitting the
		// backend (and without flooding audit/metrics). Writes flow through the
		// same wrapper, so it invalidates itself.
		storage: corscache.NewStorageMiddleware(storage),
		tracer:  otel.Tracer("internal/http/server"),
	}
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /", server.listBucketsHandler)
	apiMux.HandleFunc("HEAD /{bucket}", server.headBucketHandler)
	apiMux.HandleFunc("GET /{bucket}", server.routeBucketGetHandler)
	apiMux.HandleFunc("PUT /{bucket}", server.routeBucketPutHandler)
	apiMux.HandleFunc("DELETE /{bucket}", server.routeBucketDeleteHandler)
	apiMux.HandleFunc("OPTIONS /{bucket}", server.optionsBucketHandler)
	apiMux.HandleFunc("POST /{bucket}", server.postBucketHandler)
	apiMux.HandleFunc("HEAD /{bucket}/{key...}", server.headObjectHandler)
	apiMux.HandleFunc("GET /{bucket}/{key...}", server.getObjectOrListPartsHandler)
	apiMux.HandleFunc("POST /{bucket}/{key...}", server.createMultipartUploadOrCompleteMultipartUploadHandler)
	apiMux.HandleFunc("PUT /{bucket}/{key...}", server.uploadPartOrPutObjectHandler)
	apiMux.HandleFunc("DELETE /{bucket}/{key...}", server.abortMultipartUploadOrDeleteObjectHandler)
	apiMux.HandleFunc("OPTIONS /{bucket}/{key...}", server.optionsObjectHandler)
	var apiHandler http.Handler = apiMux
	// CORS must be wrapped *inside* the virtual-host addressing middleware so the
	// resolver sees the rewritten path (with the bucket prefix) for virtual-hosted
	// requests. This also places CORS inside SigV4 auth: anonymous preflights pass
	// through and reach this handler, but a credentialed request that fails auth
	// gets a 401 without Access-Control-* headers (an opaque CORS error in the
	// browser). Acceptable, since cross-origin requests are no-credentials by
	// default and the preflight already gates access.
	apiHandler = httpmiddleware.MakeCORSMiddlewareWithResolver(server.resolveCORSRulesForRequest, apiHandler)
	apiHandler = httpmiddleware.MakeVirtualHostBucketAddressingMiddleware(apiEndpoint, apiHandler)

	websiteMux := http.NewServeMux()
	websiteMux.HandleFunc("GET /{bucket}/{key...}", server.serveWebsiteGetObject)
	websiteMux.HandleFunc("HEAD /{bucket}/{key...}", server.serveWebsiteHeadObject)
	websiteMux.HandleFunc("GET /{bucket}", server.serveWebsiteGetObject)
	websiteMux.HandleFunc("HEAD /{bucket}", server.serveWebsiteHeadObject)
	var websiteHandler http.Handler = websiteMux

	fallbackHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		// Strip port if present
		if colonIdx := strings.LastIndex(host, ":"); colonIdx != -1 {
			if bracketIdx := strings.LastIndex(host, "]"); bracketIdx < colonIdx {
				host = host[:colonIdx]
			}
		}
		r.URL.Path = "/" + host + r.URL.Path
		slog.DebugContext(r.Context(), "Custom domain website request", "host", r.Host, "bucket", host, "path", r.URL.Path)
		websiteHandler.ServeHTTP(w, r)
	})

	// Set up handler with hostname-based routing.
	rootHandler := httpmiddleware.MakeHostnameRoutingHandler(apiEndpoint, apiHandler, websiteEndpoint, websiteHandler, fallbackHandler)
	rootHandler = makeAuditRequestContextMiddleware(rootHandler)

	var authCreds []authentication.Credentials
	if credentials != nil {
		slog.Info("Authentication is enabled")
		authCreds = sliceutils.Map(func(cred settings.Credentials) authentication.Credentials {
			return authentication.Credentials{
				AccessKeyId:     cred.AccessKeyId,
				SecretAccessKey: cred.SecretAccessKey,
			}
		}, credentials)
		rootHandler = authentication.MakeSignatureMiddleware(authCreds, region, rootHandler)
	} else {
		slog.Warn("Authentication is disabled, this is not recommended for production use")
	}
	rootHandler = httpmiddleware.MakeRequestContextMiddleware(rootHandler)

	return rootHandler
}

func makeAuditRequestContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), authentication.RequestIDContextKey{}, ulid.Make().String())
		if ip := getRemoteIP(r.RemoteAddr); ip != nil {
			ctx = context.WithValue(ctx, authentication.ClientIPContextKey{}, *ip)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func makeHealthCheckHandler(dbs []database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		for _, db := range dbs {
			err := db.PingContext(ctx)
			if err != nil {
				w.WriteHeader(503)
				w.Write([]byte("Unhealthy"))
				return
			}
		}
		w.WriteHeader(200)
		w.Write([]byte("Healthy"))
	}
}

func SetupMonitoringServer(dbs []database.Database) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", makeHealthCheckHandler(dbs))
	var rootHandler http.Handler = mux
	return rootHandler
}

const bucketPath = "bucket"
const keyPath = "key"

const maxListLimit int64 = 1000

const prefixQuery = "prefix"
const delimiterQuery = "delimiter"
const startAfterQuery = "start-after"
const maxKeysQuery = "max-keys"
const uploadIdQuery = "uploadId"
const uploadsQuery = "uploads"
const partNumberQuery = "partNumber"
const markerQuery = "marker"
const keyMarkerQuery = "key-marker"
const uploadIdMarkerQuery = "upload-id-marker"
const partNumberMarkerQuery = "part-number-marker"
const maxUploadsQuery = "max-uploads"
const maxPartsQuery = "max-parts"
const listTypeQuery = "list-type"
const continuationTokenQuery = "continuation-token"
const websiteQuery = "website"
const corsQuery = "cors"
const lifecycleQuery = "lifecycle"
const notificationQuery = "notification"
const appendQuery = "append"
const versioningQuery = "versioning"
const versionsQuery = "versions"
const versionIDQuery = "versionId"
const taggingQuery = "tagging"

const acceptRangesHeader = "Accept-Ranges"
const expectHeader = "Expect"
const contentRangeHeader = "Content-Range"
const contentLengthHeader = "Content-Length"
const rangeHeader = "Range"
const ifMatchHeader = "If-Match"
const ifNoneMatchHeader = "If-None-Match"
const etagHeader = "ETag"
const lastModifiedHeader = "Last-Modified"
const contentTypeHeader = "Content-Type"
const contentMd5Header = "Content-MD5"
const locationHeader = "Location"
const checksumTypeHeader = "x-amz-checksum-type"
const checksumAlgorithmHeader = "x-amz-sdk-checksum-algorithm"
const checksumCRC32Header = "x-amz-checksum-crc32"
const checksumCRC32CHeader = "x-amz-checksum-crc32c"
const checksumCRC64NVMEHeader = "x-amz-checksum-crc64nvme"
const checksumSHA1Header = "x-amz-checksum-sha1"
const checksumSHA256Header = "x-amz-checksum-sha256"
const writeOffsetBytesHeader = "x-amz-write-offset-bytes"
const versionIDHeader = "x-amz-version-id"
const deleteMarkerHeader = "x-amz-delete-marker"

const copySourceHeader = "x-amz-copy-source"
const copySourceVersionIDHeader = "x-amz-copy-source-version-id"
const copySourceRangeHeader = "x-amz-copy-source-range"
const metadataDirectiveHeader = "x-amz-metadata-directive"
const copySourceIfMatchHeader = "x-amz-copy-source-if-match"
const copySourceIfNoneMatchHeader = "x-amz-copy-source-if-none-match"
const copySourceIfModifiedSinceHeader = "x-amz-copy-source-if-modified-since"
const copySourceIfUnmodifiedSinceHeader = "x-amz-copy-source-if-unmodified-since"

const metadataDirectiveCopy = "COPY"
const metadataDirectiveReplace = "REPLACE"

const taggingHeader = "x-amz-tagging"
const taggingDirectiveHeader = "x-amz-tagging-directive"

// taggingCountHeader is the GetObject/HeadObject response header carrying the
// object's tag count. The S3 REST API user-guide page lists this as
// "x-amz-tag-count", but that is a documentation error: the canonical AWS API
// model (and therefore every AWS SDK and the real service) uses
// "x-amz-tagging-count".
const taggingCountHeader = "x-amz-tagging-count"

const taggingDirectiveCopy = "COPY"
const taggingDirectiveReplace = "REPLACE"

const cacheControlHeader = "Cache-Control"
const contentDispositionHeader = "Content-Disposition"
const contentEncodingHeader = "Content-Encoding"
const contentLanguageHeader = "Content-Language"
const expiresHeader = "Expires"
const websiteRedirectLocationHeader = "x-amz-website-redirect-location"

// userMetadataHeaderPrefix is the prefix of user-defined object metadata
// headers. Keys are stored lowercase without the prefix and returned with it.
const userMetadataHeaderPrefix = "x-amz-meta-"

const applicationXmlContentType = "application/xml"

const storageClassHeader = "x-amz-storage-class"

package testing

// Keep test image versions and multi-platform digests in one place.
// TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX=ghcr.io redirects implicit image names
// such as testcontainers/ryuk to ghcr.io/testcontainers/ryuk. Explicit non-Hub
// registries such as ghcr.io are preserved. Use registry.hub.docker.com for
// PostgreSQL to opt out: Testcontainers normalizes docker.io to an implicit
// registry and would still prepend the configured prefix to that spelling.
const (
	PostgresImage      = "registry.hub.docker.com/library/postgres:18.6-alpine3.24@sha256:898c60cb390d1085bc97bdd6a1ae968757421df394b071848b52b1c09c89d6a5"
	OpenSSHServerImage = "ghcr.io/linuxserver/openssh-server:10.3_p1-r1-ls239@sha256:0f535b0faa7448511b23528165e9fbd4f1fe563b861b3dcaa5ddb0ddbfe2254c"
)

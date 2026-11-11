# Releasing

A Go module has no registry to upload to: its version is its tag. A release is a GitHub Release.

1. In a pull request, set the new version in `version.go`. Merge it.
2. Publish a GitHub Release whose tag is `v` plus that version. Without `--target`, the tag goes on the default branch:

   ```sh
   gh release create v3.0.0-alpha.1 --generate-notes --prerelease
   ```

3. The `Release` workflow runs the CI checks on the tag, then asks proxy.golang.org and sum.golang.org for the version.
   From then on `go get github.com/braskai/rask-sdk-go/v3@v3.0.0-alpha.1` works, and pkg.go.dev shows the version some
   minutes later.

The workflow refuses a tag that is not the version in `version.go`, and a tag that is not on the default branch.
A tag that was public, even for a minute, may already be in the proxy or in someone's module cache, and once the
checksum database has a version it cannot change. So a tag is never moved or reused: if the workflow fails, fix the
cause and release the next version.

Nothing to set up: the workflow needs no secrets.

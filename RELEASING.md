# Releasing

A Go module has no registry to upload to: its version is its tag. A release is one run of the `Release` workflow, which
makes the tag only after the checks pass.

1. In a pull request, set the new version in `version.go`. Merge it.
2. Start the workflow on the default branch, with "Run workflow" on the Actions tab or from a terminal:

   ```sh
   gh workflow run release.yml
   ```

3. The workflow runs the CI checks, tags the commit `v` plus the version, makes the GitHub Release, then asks
   proxy.golang.org and sum.golang.org for the version. From then on
   `go get github.com/braskai/rask-sdk-go/v3@v<version>` works, and pkg.go.dev shows the version some minutes later.

The workflow refuses to run from any other branch, and refuses a version that is already tagged.
Do not push a version tag by hand: it would be public without the checks.
A tag that was public, even for a minute, may already be in the proxy or in someone's module cache, and once the
checksum database has a version it cannot change. So a tag is never moved or reused: to fix a version, release the next.

Nothing to set up: the workflow needs no secrets.

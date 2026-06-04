# Releasing throttlekit-py

Releases are **tag-driven** and publish to PyPI via **trusted publishing (OIDC)** — there is no
long-lived API token in the repo. Pushing a `v*` tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml):
it builds the dists, smoke-tests the wheel in a clean venv, publishes to PyPI over OIDC, then cuts the
matching GitHub Release from the CHANGELOG.

## One-time PyPI setup (trusted publisher)

Do this **once**, before the next release (the publish job fails `invalid-publisher` until it's done).
The `throttlekit-py` project already exists on PyPI, so configure the publisher under the project itself:

> PyPI → **throttlekit-py** → *Manage* → *Publishing* → **Add a new publisher** (GitHub Actions):
>
> | Field | Value |
> |---|---|
> | Owner | `AmeyaBorkar` |
> | Repository name | `throttlekit-py` |
> | Workflow filename | `release.yml` |
> | Environment | `release` |

The `release` value must match the `environment: release` on the workflow's publish job. GitHub
auto-creates that environment on first run; optionally pre-create it under *Settings → Environments* to
add required-reviewer or branch protections that gate who can publish.

Once a trusted-publishing release succeeds, the old `PYPI_API_TOKEN` repo secret is unused and can be
deleted (`gh secret delete PYPI_API_TOKEN -R AmeyaBorkar/throttlekit-py`).

## Cutting a release

1. Land the change on `main` and make sure CI is green.
2. Bump `version` in [`pyproject.toml`](pyproject.toml) (and `src/throttlekit/_version.py` if it carries
   the version) and add a dated section to [`CHANGELOG.md`](CHANGELOG.md) under a `## [X.Y.Z] — DATE`
   header (the GitHub Release notes are extracted from it).
3. Tag and push:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

4. The workflow publishes to PyPI and creates the GitHub Release. Verify:

   ```bash
   pip index versions throttlekit-py     # or: https://pypi.org/project/throttlekit-py/
   ```

Re-running a tag is safe: PyPI upload uses `skip-existing: true` (a duplicate version is a no-op
success, never overwritten), and the GitHub Release step updates in place.

## Versioning

This client versions **independently** of the Node core. It tracks a checksum-pinned *contract* (the
`.proto`, golden vectors, and extracted Lua); a behavioral break in the core bumps the pinned
`contractVersion` (see [`tests/test_contract.py`](tests/test_contract.py)), which is matched here
deliberately. While the package is pre-1.0 (alpha), minor versions may carry breaking client-API changes.

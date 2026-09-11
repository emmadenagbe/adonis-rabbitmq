# Contributing

## Commit messages

This repo uses [Conventional Commits](https://www.conventionalcommits.org/), enforced by commitlint on every commit (`.husky/commit-msg`). The version bump and changelog on release are generated directly from this history, so the type you pick matters.

```
<type>(<scope>)?: <description>

<body>?

<footer>?
```

Common types:

- `fix:` — a bug fix → patch release (0.1.0 → 0.1.1)
- `feat:` — a new feature → minor release (0.1.0 → 0.2.0)
- `feat!:` or a footer of `BREAKING CHANGE: ...` → major release (0.1.0 → 1.0.0)
- `chore:`, `docs:`, `refactor:`, `test:`, `ci:` — no release triggered on their own

Examples:

```
fix: correctly ack messages routed to the dead-letter exchange
feat: support binding a queue to multiple routing keys
feat!: rename `connection` option to `connectionName` on @consumer

BREAKING CHANGE: consumers must now use `connectionName` instead of `connection`
```

## Releasing

Releases are cut from GitHub Actions (`.github/workflows/release.yml`, manually triggered via `workflow_dispatch`), never from a local machine. It runs lint/typecheck/test, then `npm run release` (`release-it`), which:

1. Reads commits since the last tag to determine the next version (patch/minor/major)
2. Updates `CHANGELOG.md`
3. Commits, tags (`vX.Y.Z`), and pushes
4. Publishes to npm and creates a GitHub release

Nothing needs to be bumped by hand — the commit history is the source of truth for the version.

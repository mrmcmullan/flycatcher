# GitHub Workflows & Automation

This directory contains all GitHub Actions workflows, issue templates, and automation
configuration for the `flycatcher` project.

## Workflows

### CI (`ci.yml`)

**Triggers:** PR, push to main, manual

**What it does:**

- Runs tests on Ubuntu with Python 3.12 (Windows/macOS can be added later)
- Enforces 70% minimum code coverage
- Runs linting (ruff) and formatting checks
- Type checking with mypy (enforced)
- Uploads coverage to Codecov

**Required for merge:** Yes

### Documentation (`docs.yml`)

**Triggers:** PR (docs/), push to main (docs/), manual

**What it does:**

- **On PRs:** Tests that docs build successfully, uploads preview artifact
- **On main push:** Builds and deploys documentation to GitHub Pages

**Required for merge:** Yes (for PRs touching docs)

### PR Labeler (`pr-labeler.yml`)

**Triggers:** PR opened/edited/synced

**What it does:**

- Automatically labels PRs based on changed files (using `.github/labeler.yml`)
- Validates PR title follows conventional commits format (build, chore, ci, depr, docs, feat, fix, perf, refactor, release, test)
- Enforces title starts with uppercase letter and doesn't end with punctuation

**Configuration:** `.github/labeler.yml`

### Release Drafter (`release-drafter.yml`)

**Triggers:** Push to main, PR merged to main, manual

**What it does:**

- Automatically maintains a draft release with changelog
- Categorizes PRs by type (Features, Bug Fixes, Documentation, etc.)
- Auto-increments version based on labels (major/minor/patch)
- Lists contributors

**Configuration:** `.github/release-drafter.yml`

### Publish to PyPI (`publish.yml`)

**Triggers:** Release published, manual

**What it does:**

- Builds Python package distribution
- Publishes to TestPyPI (manual trigger with environment selection)
- Publishes to PyPI (on GitHub release or manual)
- Uses trusted publishing (no API tokens needed)

**Required secrets:** None (uses trusted publishing with OIDC)

## Issue Templates

Located in `.github/ISSUE_TEMPLATE/`:

- **Bug Report** (`bug_report.yml`) - Report bugs with reproduction steps
- **Feature Request** (`feature_request.yml`) - Suggest new features
- **Documentation Improvement** (`documentation.yml`) - Suggest improvements to the documentation
- **Chore Tracker** (`chore_tracker.yml`) - Track maintenance work
- **Question** (`question.yml`) - Ask usage or implementation questions

**Configuration:** `.github/ISSUE_TEMPLATE/config.yml`

## PR Template

Located at `.github/pull_request_template.md`

Encourages good PR descriptions with:

- Type of change
- Related issues
- Testing checklist
- Review checklist

## Labels Configuration

### File-based auto-labeling (`.github/labeler.yml`)

- `documentation` - Changes to docs/**, *.md, mkdocs.yml
- `tests` - Changes to tests/**
- `core` - Changes to base.py
- `fields` - Changes to fields.py
- `validators` - Changes to validators.py
- `generators` - Changes to generators/**
- `dependencies` - Changes to pyproject.toml, uv.lock
- `ci` - Changes to .github/**

### PR title-based labels (via release-drafter)

- `bug`/`fix` - Bug fixes
- `enhancement`/`feat` - New features
- `chore` - Maintenance work
- `test` - Test improvements
- `ci` - CI/CD changes

## Branch Protection Rules (Recommended)

For `main` branch:

- ✅ Require pull request before merging
- ✅ Require status checks to pass:
  - `Test Python 3.12 on ubuntu-latest`
  - `Lint and format check`
  - `Test documentation build` (if docs changed)
- ✅ Require conversation resolution before merging
- ✅ Require linear history (squash merging)
- ❌ Don't require review approvals (for solo maintainer)

## Release Process

### For maintainers

1. **During development:**
   - PRs are automatically added to draft release
   - Label PRs appropriately (feat, fix, etc.)

2. **When ready to release:**
   - Review draft release notes at https://github.com/mrmcmullan/flycatcher/releases
   - Edit version number if needed
   - Edit release notes for clarity
   - Click "Publish release"

3. **Automatic publishing:**
   - Publishing the release triggers `publish.yml`
   - Package is built and published to PyPI

### Testing releases

```bash
# Manual trigger to test on TestPyPI first
# Go to Actions → Publish to PyPI → Run workflow
# Select "testpypi" environment
```

## Setup Required

### Repository Settings

1. **Enable GitHub Pages:**
   - Go to Settings → Pages
   - Source: GitHub Actions
   - No custom domain needed initially

2. **Enable Discussions (optional but recommended):**
   - Go to Settings → Features
   - Check "Discussions"

3. **Configure PyPI Publishing (trusted publishing):**
   - Go to https://pypi.org/manage/account/publishing/
   - Add publisher:
     - PyPI Project Name: `flycatcher`
     - Owner: `mrmcmullan`
     - Repository: `flycatcher`
     - Workflow: `publish.yml`
     - Environment: `pypi`
   - Repeat for TestPyPI at https://test.pypi.org

4. **Add Codecov (optional):**
   - Go to https://codecov.io
   - Connect GitHub repository
   - Add `CODECOV_TOKEN` secret to repository

### Environments

Create these environments in Settings → Environments:

1. **pypi**
   - No secrets needed (trusted publishing)
   - Optional: Add protection rules

2. **testpypi**
   - No secrets needed (trusted publishing)

## Workflow Dependencies

```
PR opened
  → ci.yml (tests, lint)
  → pr-labeler.yml (labels, title check)
  → docs.yml (if docs changed)

PR merged to main
  → release-drafter.yml (updates draft release)
  → docs.yml (deploys to GitHub Pages)

Release published
  → publish.yml (publishes to PyPI)
```

## Troubleshooting

### CI fails with "Coverage too low"
- Run tests locally: `uv run pytest --cov=flycatcher --cov-report=term-missing`
- Add tests to increase coverage above 70%

### PR title validation fails
- Ensure title starts with: `build:`, `chore:`, `ci:`, `depr:`, `docs:`, `feat:`, `fix:`, `perf:`, `refactor:`, `release:`, or `test:`
- First letter after colon must be uppercase: `feat: Add new feature` ✅ not `feat: add new feature` ❌
- Title must not end with punctuation: `feat: Add new feature` ✅ not `feat: Add new feature.` ❌

### PyPI publishing fails
- Ensure trusted publishing is configured correctly
- Check version in `pyproject.toml` isn't already published
- Review workflow logs for specific error

### Docs deployment fails

- Check that mkdocs.yml exists and is valid
- Ensure GitHub Pages is enabled
- Review `docs.yml` workflow logs

## Future Enhancements

- [ ] Add benchmark tracking (for performance regression detection)
- [ ] Add security scanning (Dependabot, CodeQL)
- [ ] Add stale issue management
- [ ] Add automated dependency updates
- [ ] Add weekly/monthly metrics reporting


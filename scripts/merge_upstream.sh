#!/usr/bin/env bash
# Merge an upstream Electric ref without bringing back the upstream code this fork removed.
#
# Runs `git merge --no-commit`, removes every path in fork/removed-upstream-paths again (conflicted,
# modified or newly added alike), regenerates the lockfile once no conflict is left, and leaves the
# merge staged for review.
# It then prints what still needs a person: conflicts, new upstream paths, and changesets that bump a
# package this workspace no longer has.
#
# Usage: scripts/merge_upstream.sh <ref>   e.g. upstream/main or @core/sync-service@1.8.1
#        (run `git fetch upstream --tags` first)
set -euo pipefail

ref="${1:?usage: merge_upstream.sh <ref>, e.g. upstream/main or @core/sync-service@1.8.1}"
cd "$(git rev-parse --show-toplevel)"
removed=fork/removed-upstream-paths

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "commit or stash your changes before merging" >&2
  exit 1
fi

git merge --no-ff --no-commit "$ref" || git rev-parse -q --verify MERGE_HEAD >/dev/null
if ! git rev-parse -q --verify MERGE_HEAD >/dev/null; then
  echo "nothing to merge from $ref"
  exit 0
fi

git rm -r -q -f --ignore-unmatch --pathspec-from-file="$removed"

if [ -n "$(git ls-files -u -- pnpm-lock.yaml)" ]; then
  git checkout --ours -- pnpm-lock.yaml
  git add pnpm-lock.yaml
fi

# The merged tree is not reviewed yet: pnpm must not load a .pnpmfile.cjs or run a script from it.
lockfile_only="pnpm install --lockfile-only --ignore-pnpmfile --ignore-scripts"

conflicts="$(git diff --name-only --diff-filter=U)"
if [ -z "$conflicts" ]; then
  $lockfile_only
  git add pnpm-lock.yaml
fi

new_paths="$(git diff --cached --name-only --diff-filter=A HEAD |
  awk -F/ '$1 == "packages" { print $1 "/" $2; next } $1 == ".github" || $1 == "scripts" { print; next } { print $1 }' |
  sort -u | while read -r path; do git cat-file -e "HEAD:$path" 2>/dev/null || echo "$path"; done)"

workspace_packages="$(node -e 'for (const f of process.argv.slice(1)) console.log(require("./" + f).name)' packages/*/package.json)"
stale_changesets="$(git diff --cached --name-only --diff-filter=A HEAD -- '.changeset/*.md' |
  while read -r file; do
    sed -n '2,/^---$/s/^["'\'']\([^"'\'']*\)["'\'']: .*/\1/p' "$file" | while read -r name; do
      grep -qxF "$name" <<<"$workspace_packages" || echo "$file bumps $name"
    done
  done)"

if [ -n "$new_paths" ]; then
  printf '\nNew upstream paths. Keep them, or add them to %s and run git rm -r on them:\n%s\n' "$removed" "$new_paths"
fi
if [ -n "$stale_changesets" ]; then
  printf '\nChangesets for packages this workspace no longer has (drop the line or the file):\n%s\n' "$stale_changesets"
fi
if [ -n "$conflicts" ]; then
  printf '\nConflicts left. Resolve them, run %s, git add, then git commit:\n%s\n' "$lockfile_only" "$conflicts"
  exit 1
fi
printf '\nMerge of %s staged. Review it, then git commit.\n' "$ref"

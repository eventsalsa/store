---
name: code-review
description: Checklist for conducting code reviews, validating architectural coherence, checking correctness, and ensuring test coverage.
---

# Code Review & Verification

This skill defines the checks and procedures to perform before merging code changes.

## Priorities for Verification

### 1. Leftovers & Dead Code
Agentic coding can leave behind artifacts. Ensure the following are removed if no longer used:
*   Deprecated structs, fields, or methods.
*   Old interfaces that were replaced.
*   Orphaned files or test helpers.
*   Unused imports.
*   Comments referencing deprecated designs.

### 2. Architectural Coherence
*   Verify new code adheres to the package structure and doesn't introduce parallel patterns.
*   Ensure package boundaries make sense and dependencies are not introduced unnecessarily.

### 3. Correctness & Security
*   **SQL Safety**: Ensure all queries are parameterized to prevent SQL injection. Check for proper indexing.
*   **Transaction Boundaries**: Ensure the library does not call `Commit()` or `Rollback()` directly on `*sql.Tx` (callers control boundaries).
*   **Concurrency**: Ensure shared mutable state is guarded against race conditions.

### 4. API Quality
*   Verify the public API is kept minimal.
*   Ensure that every public function has accurate, complete doc comments.

## Quality Commands

```bash
# Run the linter
golangci-lint run --timeout=5m

# Run unit tests to check regression
go test ./...
```

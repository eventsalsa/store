---
name: go-development
description: Guidelines for Go development in the project, covering packages, interface design, error handling, concurrency, and style rules.
---

# Go Development & Implementation Guidelines

This skill defines the development guidelines and coding style conventions for implementing features in Go.

## Code Style & Conventions

### 1. Interface Design
*   Define interfaces before implementing them. Keep them minimal and focused on single responsibilities.
*   Place core interfaces in the root `store` package.
*   All database operations must accept `context.Context` as the first parameter.
*   All database operations must accept `*sql.Tx` as the second parameter (never `*sql.DB`).

### 2. Error Handling
*   Return clear, specific sentinel errors (e.g., `store.ErrOptimisticConcurrency`, `store.ErrNoEvents`).
*   Wrap errors with context: `fmt.Errorf("failed to append events: %w", err)`.
*   Always check and propagate errors; never silently discard them.

### 3. Naming Conventions
*   Event types: Use descriptive past-tense nouns (e.g., `UserCreated`, `OrderPlaced`).
*   Aggregate types: Use singular nouns (e.g., `User`, `Order`).
*   Package names: Short, lowercase, no underscores.
*   Config structs: Name as `XxxConfig` and provide a `DefaultXxxConfig()` constructor.

### 4. Concurrency
*   Write concurrent code using standard Go patterns (`sync.WaitGroup`, channels).
*   Avoid sharing memory; share data by communicating where possible.
*   Use optimistic concurrency for event store writes via `ExpectedVersion`.

### 5. Line Length & Comments
*   Limit lines to a maximum of 120 characters.
*   Maintain doc comments on all exported structs, interfaces, methods, and functions.
*   Preserve all existing comments and docstrings when making changes.

### 6. Logging
*   Logger is optional in config structs. Always guard calls: `if config.Logger != nil { ... }`.

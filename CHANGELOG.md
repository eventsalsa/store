# Changelog

## [0.1.0](https://github.com/eventsalsa/store/compare/v0.0.4...v0.1.0) (2026-08-20)


### ⚠ BREAKING CHANGES

* **store:** replace aggregate terminology with stream across codebase

### Features

* range partitioning on global_position & authoritative stream_heads concurrency control ([#11](https://github.com/eventsalsa/store/issues/11)) ([ca1319c](https://github.com/eventsalsa/store/commit/ca1319ca3b7b8885e53f9e8d538915d9b5abc4ab))


### Code Refactoring

* **store:** replace aggregate terminology with stream across codebase ([#7](https://github.com/eventsalsa/store/issues/7)) ([b22759d](https://github.com/eventsalsa/store/commit/b22759d594e1d2ee36df95e22b60466e427212c0))

## [0.0.4](https://github.com/eventsalsa/store/compare/v0.0.3...v0.0.4) (2026-08-14)


### Bug Fixes

* **eventmap:** ensure deterministic FromESEvent switch cases and gofmt output ([#4](https://github.com/eventsalsa/store/issues/4)) ([aa4e6af](https://github.com/eventsalsa/store/commit/aa4e6af3b98d8b1c7b9392309839301075f430a2))

## [0.0.3](https://github.com/eventsalsa/store/compare/v0.0.2...v0.0.3) (2026-06-07)

### Bug Fixes

* **postgres:** support simple protocol JSONB parameter binding ([#3](https://github.com/eventsalsa/store/pull/3))

## [0.0.2](https://github.com/eventsalsa/store/compare/v0.0.1...v0.0.2) (2026-06-07)

### Code Refactoring

* **postgres:** migrate database/sql to native pgx/v5 ([#2](https://github.com/eventsalsa/store/pull/2))

### Miscellaneous Chores

* **agents:** convert GitHub Copilot instructions to workspace agent skills ([#1](https://github.com/eventsalsa/store/pull/1))

## 0.0.1 (2026-04-16)

### Features

* add initial eventsalsa store library

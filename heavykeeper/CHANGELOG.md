# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.9...v0.7.0) - 2026-06-29

### Added

- return (evicted, inserted) from add_with_evicted

### Fixed

- don't track zero-count items in add_with_evicted

### Other

- Merge pull request #89 from detemmienation/mem

## [0.6.9](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.8...v0.6.9) - 2026-06-19

### Added

- add mem_bytes() to TopK and BucketedTopK
- add mem_bytes() to CuckooTopK

### Other

- rename query to contains and deprecate query
- Rename the helper and add tests

## [0.6.8](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.7...v0.6.8) - 2026-06-17

### Added

- derive Clone for TopK
- derive Clone for BucketedTopK
- derive Clone for CuckooTopK

### Fixed

- seed decay RNG in with_seed for reproducibility

## [0.6.7](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.6...v0.6.7) - 2026-06-07

### Added

- rename to contains_top_k
- add query_topk_items to TopK and BucketedTopK
- use contains for top-k checks
- *(cuckoo)* add query_topk_items to check top-k

## [0.6.6](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.5...v0.6.6) - 2026-06-04

### Added

- add add_with_evicted to TopK and BucketedTopK
- *(cuckoo)* add add_with_evicted

### Other

- Avoid extra clone of evicted key
- Return evicted item as Option<T>

## [0.6.5](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.4...v0.6.5) - 2026-05-14

### Other

- Dropping trait bound on fmt::Debug for crate main structs:

## [0.6.4](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.3...v0.6.4) - 2026-05-10

### Added

- add CuckooTopK variant

### Fixed

- address CodeRabbit review on PR #70

## [0.6.3](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.2...v0.6.3) - 2026-04-26

### Added

- add experimental BucketedTopK

### Other

- Merge pull request #68 from pmcgleenon/add-bucketed-topk
- *(deps)* update mockall requirement from 0.13 to 0.14
- Merge pull request #61 from pmcgleenon/dependabot/github_actions/actions/checkout-6
- *(deps)* update criterion requirement from 0.7.0 to 0.8.2

## [0.6.2](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.1...v0.6.2) - 2025-11-19

### Other

- Improve TopKQueue::iter perf + add list benchmark
- Fix decay probability scaling and add test

## [0.6.1](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.6.0...v0.6.1) - 2025-09-02

### Other

- added Sync to rng
- Merge pull request #52 from pmcgleenon/dependabot/cargo/criterion-0.7.0

## [0.6.0](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.5.1...v0.6.0) - 2025-07-27

### Other

- moved word count to examples & updated dependencies
- accept borrowed values

## [0.5.1](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.5.0...v0.5.1) - 2025-06-27

### Other

- Merge pull request #44 from pmcgleenon/dependabot/cargo/mockall-0.13
- added Send trait to  rng

## [0.5.0](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.4.0...v0.5.0) - 2025-06-20

### Other

- updated decay logic + added tests
- add increment argument to TopK::add

## [0.4.0](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.3.1...v0.4.0) - 2025-05-31

### Other

- Merge pull request #37 from pmcgleenon/dependabot/cargo/criterion-0.6.0
- [**breaking**] use reference in add method to avoid cloning

## [0.3.1](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.3.0...v0.3.1) - 2025-04-12

### Other

- fixed clippy warning for manual_hash_one
- removed unused dependency

## [0.3.0](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.2.7...v0.3.0) - 2025-03-30

### Added

- try hash composition instead of recalculating new hash
- replaced external priority queue with internal implementation


## [0.2.7](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.2.6...v0.2.7) - 2025-03-15

### Fixed

- fixed word counting app

### Other

- added script for test data, README tidy-ups

## [0.2.6](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.2.5...v0.2.6) - 2025-02-11

### Added

- added merge API to merge heavykeeper structs

### Other

- added badges for crate, license, github status

## [0.2.5](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.2.4...v0.2.5) - 2025-02-06

### Other

- random - dependabot upgrade
- Update rand requirement from 0.8.5 to 0.9.0
- Update rand_distr requirement from 0.4.3 to 0.5.0

## [0.2.4](https://github.com/pmcgleenon/heavykeeper-rs/compare/v0.2.3...v0.2.4) - 2024-11-30

### Other

- Switched to 64 bit counts
- refactored main program
- used mmap for reading file
- modified string processing
- updated count test

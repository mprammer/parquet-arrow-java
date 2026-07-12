# Security policy

## Reporting a vulnerability

If you believe you've found a security issue in parquet-arrow-java, please report it
privately rather than opening a public issue.

Preferred channel: **GitHub's private vulnerability reporting** —
[open an advisory](https://github.com/mprammer/parquet-arrow-java/security/advisories/new)
from the repo's Security tab. Reports submitted that way are visible only to
the maintainers.

Fallback: email **parquet-arrow@spiraldb.com** with subject prefix `[security]`.

Please include:

- A description of the issue and its impact.
- Steps to reproduce, or a minimal proof-of-concept.
- The commit SHA or release version where you observed the issue.

## What to expect

- We aim to acknowledge reports within **14 days**.
- Medium-or-higher-severity issues, once confirmed, are patched within
  **60 days** of acknowledgement; critical issues are patched as quickly as we
  can.
- Once a fix ships, we'll publish an advisory crediting the reporter (unless
  you'd rather stay anonymous).

## Scope

The library code (everything under `parquet-arrow-core/` and
`parquet-arrow-conformance/`) is in scope, including the vendored
`org.apache.hadoop.*` shim classes. Note that parquet-arrow-java depends on
`parquet-java` and Apache Arrow Java; vulnerabilities originating in those
upstream libraries should be reported to their respective projects, though
we're happy to help route a report.

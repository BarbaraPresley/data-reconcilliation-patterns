# Org Entity Resolution

This folder contains examples of reconciliation logic used to resolve organizational relationships across API endpoints.

These scripts demonstrate how hierarchical entity data can be discovered, validated, and aligned into analytics-ready structures suitable for downstream systems.

---

## Contents

### org_crawl_v1_direct_reports.py

Initial working implementation of an organizational hierarchy crawl using a direct-reports endpoint.

Demonstrates:

- hierarchical traversal of nested relationships
- normalization of nested JSON responses
- basic CSV export of structured org data
- foundational identity mapping patterns

---

### org_crawl_v2_enriched_reconciliation.py

Refactored and extended version of the org crawl, introducing modular request handling, QA validation checks, endpoint reconciliation, and schema alignment for downstream systems.

Enhancements include:

- reusable API session management
- queue-based traversal of hierarchical relationships
- reconciliation of entity records across multiple endpoints
- structured QA checks for duplicates and null identifiers
- transformation to a Microsoft-ready relational schema
- deterministic export structure for repeatable workflows

---

## What this demonstrates

- iterative traversal of hierarchical entity relationships
- resolving identity records across multiple API endpoints
- validation of join logic and referential integrity
- QA instrumentation for reliability
- transforming raw API output into downstream-ready table structures
- progressive refactoring toward modular, production-style code organization

---

## Context

Patterns shown here commonly appear in:

- HRIS integrations
- CRM entity synchronization
- identity resolution workflows
- master data management pipelines
- analytics engineering data preparation layers

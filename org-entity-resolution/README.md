# Org Entity Resolution

This folder contains examples of reconciliation logic used to resolve organizational relationships across API endpoints.

## Contents

- `org_crawl_v1_direct_reports.py`  
  Basic working version of an org hierarchy crawl using a direct-reports endpoint.

- `org_crawl_v2_enriched_reconciliation.py`  
  Refactored version with modular request handling, QA checks, endpoint reconciliation, and downstream schema alignment.

## What this demonstrates

- iterative traversal of hierarchical relationships
- resolving entity records across multiple API endpoints
- basic QA and reconciliation checks
- transforming raw API output into a downstream-ready table structure

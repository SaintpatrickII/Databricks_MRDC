# Databricks_MRDC
Migration of MRDC project to Databricks


Data engineering is the development, implementation, and maintenance of systems and processes that take in raw data and produce high-quality, consistent information that supports downstream use cases, such as analysis and machine learning. Data engineering is the intersection of security, data management, DataOps, data architecture, orchestration, and software engineering. A data engineer manages the data engineering lifecycle, beginning with getting data from source systems and ending with serving data for use cases, such as analysis or machine learning.


**shall we begin?**

## Inital Setup:

- Before we begin constucting the connectors to various dataset sources, best practice dictates hiding secrets from public eye
- With databricks we can create a scope for secrets & add secrets to this scope via the API CLI

``` bash
databricks secrets create-scope user_scope
databricks secrets put-secret --json '{
  "scope": "user_scope",
  "key": "db_password",
  "string_value": "my_db_password"
}'

```

## Raw->Bronze:

- Source postgres table connected
- Orders Fact table:
    - Removed PII
    - order_quantity enforced as int


# snow-transcribe
- A collection of scripts to transcribe snowflake account and object information to various formats

### Supported transcriptions
- Snowflake -> Terraform
- Snowflake -> Snowflake


## Snowflake -> Terraform
- Transcribe snowflake users, roles, and role grants to terraform resource format

#### Supported Resources
- Users
- Roles
- Role Grants


## Snowflake -> Snowflake
- Replicates account and database objects from one account to another

#### Supported Objects
- Database Objects (get ddl)
  - Options to specify:
    - SCHEMA
    - TABLE
    - VIEW – includes materialized views  

- Account Objects
  - Warehouse
  - Users
  - Roles
  - Grants
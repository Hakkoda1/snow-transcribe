# Account Object Replication Process

## Before Running Script
1. Create a temporary rsa user with accountadmin role in the source account
2. Create a new account in the same organization
3. Create a temporary rsa user with accountadmin role in the target account
4. Create/Update the .config file with the credentials for the source and target account

## Running The Script
1. Define objects that should be copied over
2. Run the script
3. Review any objects that couldn't be created (exception handling not added yet)

### Inputs
- Config file (with source and target account info)
- source_config_name (default is "snowflake_source_account", see example_cred.config)
- target_config_name (default is "snowflake_target_account", see example_cred.config)

### Object Options
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

- Not supported yet:
    - STREAM
    - SEQUENCE
    - PIPE
    - FILE_FORMAT
    - FUNCTION – User Defined Functions
    - PROCEDURE – stored procedures  
    - Future Grants

### Dependencies
- Pandas
- Snowflake connector
- Configparser
- regex

### Considerations
- The intended usage is to update a brand new Snowflake account with the account and database objects of another account. It is not intended to update an existing account. 
- Created user accounts will created with a default password ("pass123") unless manually specified (SSO is not preserved)
- The "grantee" of roles/users will not be preserved as the temporary user will create all the objects and grants
- The accountadmin will be the owner of all new objects (to be udpdated in future versions)
- Exception handling in progress
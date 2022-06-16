# Account Object Replication Process


## Before Running Script
1. Create a temporary rsa user with accountadmin role in the source account
2. Create a new account in the same organization
3. Create a temporary rsa user with accountadmin role in the target account
4. Create/Update the .config file with the credentials for the source and target account
Note: (you can also use a regular user + password for authentication, but it is not recommended)

## Running The Script
1. Define objects that should be copied over
2. Run the script
3. Review any objects that couldn't be created (exception handling is basic currently)
4. *Delete the rsa users created for the account replication process

### Inputs
- Required
    - Config file (string: path for config file with source and target account info)
- Optional
    - source_config_name (string: default is "snowflake_source_account", see example_cred.config)
    - target_config_name (string: default is "snowflake_target_account", see example_cred.config)
    - conn_type_source (string: connection type for source account, default is private_key (rsa auth))
    - conn_type_target (string: connection type for target account, default is private_key (rsa auth))
    - db_ignore_list (list of strings: Pass a list of databases to ignore, default is none)
    - return_sql (bool: true returns the sql statments that are being executed, default is True)

### Object Options
- Database Objects (get ddl)
    - SCHEMA
    - TABLE  

- Account Objects
  - Warehouse
  - Users
  - Roles
  - Grants

- Not supported yet:
    - VIEWS
    - MATERIALIZED VIEWS
    - SECURE VIEWS
    - STREAM
    - STAGE
    - SEQUENCE
    - PIPE
    - FILE_FORMAT
    - FUNCTION – User Defined Functions
    - TASK
    - PROCEDURE – stored procedures  
    - Future Grants
    - Objects with masking policies
    - Objects with references

### Dependencies
- Pandas
- Snowflake connector
- Configparser
- regex
- cryptography (for rsa keys)

### Considerations
- The intended usage is to update a brand new Snowflake account with the account and database objects of another account. It is not intended to update an existing account. 
- Created user accounts will created with a default password ("pass123") unless manually specified (SSO is not preserved)
- The "grantee" of roles/users will not be preserved as the temporary user will create all the objects and grants
- The accountadmin will be the owner of all new objects (to be udpdated in future versions)
- Exception handling in progress

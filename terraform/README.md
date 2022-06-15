# Terraform Config File Creation

### (Status)
- Needs additional Testing

### Purpose
- Transcribe snowflake users, roles, and role grants to terraform format
- Currently terraform can import state, but cannot generate a config file from the imported state
- To be used with snowflake provider (snowflake-labs/snowflake)

### Usage
1. Create 'snowflake.config' file with account information (ex: see example_creds.config file)
2. Run script
3. Review genereated files


### Outputs
- roles text file
- contains snowflake roles in terraform "snowflake_role" resource format
- users text file
- contains snowflake users in terraform "snowflake_user" resource format
- grants text file
- contains snowflake grants for roles in terraform "snowflake_role_grants" resource format


### Dependencies
- pandas
- snowflake.connector
- configparser




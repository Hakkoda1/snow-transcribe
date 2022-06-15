import pandas as pd
import snowflake.connector 
import configparser
import re


"""
NOTES

## Before Running Script
1. Create a temporary rsa user with accountadmin role in the source account
2. Create a new account in the same organization
3. Create a temporary rsa user with accountadmin role in the target account
4. Create/Update the .config file with the credentials for the source and target account

## Running The Script
1. Define objects that should be copied over
2. Run the script
3. Review any objects that couldn't be created (exception handling not added yet)
4. *Delete the users created for the account replication process

### Inputs
- Config file (with source and target account info)
- source_config_name (default is "snowflake_source_account", see example_cred.config)
- target_config_name (default is "snowflake_target_account", see example_cred.config)

### Object Options
- Database Objects (get ddl)
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
"""




def parse_credentials(config_file, config_name):
    """ helper function to create snowflake connection and cursor """

    credentials = configparser.ConfigParser()
    credentials.read(config_file)

    user = credentials[config_name]['user']
    password = credentials[config_name]['password']
    account = credentials[config_name]['account']
    warehouse = credentials[config_name]['warehouse']

    conn = snowflake.connector.connect(
            user = user,
            password = password,
            account = account,
            warehouse = warehouse
        )

    cur = conn.cursor()
    
    return conn, cur


class transcribe_snowflake_account:
    """
    A class for copying objects from one snowflake account to another

    Attributes:
        config_file : str
            the path of the config file that contains snowflake credentials for two accounts
            currently only password authentication is supported
        source_config_name : str
            the name of the header in the config file for the snowflake source account credentials
        target_config_name : str
            the name of the header in the config file for the snowflake source account credentials

    """
    def __init__(self, config_file, source_config_name='snowflake_source_account',
                 target_config_name='snowflake_target_account'):
        """ Inits class with cursors and connections for snowflake accounts """
        self.source_conn, self.source_cur = parse_credentials(config_file, source_config_name)
        self.target_conn, self.target_cur = parse_credentials(config_file, target_config_name)
        
        
    def database_objects(self):
        """ - Reads databases from the source account
            - Creates databases in the target account
            - Outputs a list of sql for dropping objects
        """

        sql = 'show databases'
        df_db = pd.read_sql(sql, self.source_conn)

        # Don't include default snowflake databases:
        databases = df_db[(df_db['name'] != 'SNOWFLAKE') & (df_db['name'] != 'SNOWFLAKE_SAMPLE_DATA')]['name'].unique().tolist()

        # Get + execute ddl for all objects in one database
        for database in databases:
            sql = f"""select get_ddl('database', '{database}', true)"""
            print(sql)

            df_db_ddl = pd.read_sql(sql, self.source_conn)
            list_of_commands = [x for x in [ re.sub(r"[\n\t]*", "", x) for x in df_db_ddl.iloc[0,0].split(";") ]  if x ]
            print(list_of_commands)

            # loop through ddl commands
            [self.target_cur.execute(sql) for sql in list_of_commands]

        db_drop_sql_list = [f"""DROP DATABASE IF EXISTS '{database}';""" for database in databases]
        print("created db objects")
        
        return db_drop_sql_list
    
    
    def roles(self):
        """ - Reads roles from the source account
            - Creates roles in the target account
            - Outputs a list of sql for dropping objects
        """
        # don't re-create default roles
        sql = """select * from snowflake.account_usage.roles
                    where name not like 'PUBLIC' and
                    name not like 'ACCOUNTADMIN' and
                    name not like 'SECURITYADMIN' and
                    name not like 'ORGADMIN' and
                    name not like 'USERADMIN' and
                    name not like 'SYSADMIN';"""

        df_roles = pd.read_sql(sql, self.source_conn)
        roles = df_roles['NAME'].values.tolist()
        
        roles_sql =  [f"""CREATE OR REPLACE ROLE {role}""" for role in roles]
        [self.target_cur.execute(sql) for sql in roles_sql]
        
        drop_roles_sql_list = [f"""DROP ROLE IF EXISTS {role};""" for role in roles]
        print("created roles")
        
        return drop_roles_sql_list
      
        
    def users(self):
        """ - Reads users from the source account
            - Creates users in the target account
            - Outputs a list of sql for dropping objects
            - Users created with: name, login_name, display_name, default_role, email
        """

        # Ingore default snowflake role and the user who was used to create the account
        sql = """select * from snowflake.account_usage.users
                where name not like 'SNOWFLAKE' and
                created_on not in (SELECT min(created_on) FROM snowflake.account_usage.users);"""

        df_users = pd.read_sql(sql, self.source_conn)
        
        names = df_users['NAME'].values.tolist()
        login_names = df_users['LOGIN_NAME'].values.tolist()
        display_names = df_users['DISPLAY_NAME'].values.tolist()
        default_roles = df_users['DEFAULT_ROLE'].values.tolist()
        emails = df_users['EMAIL'].values.tolist()

        # Construct user strings dynamically
        for i in range(0, len(df_users)):

            name = f'"{names[i]}"'
            password = "'abc123'"

            if login_names[i] != None:
                login_name = f"login_name='{login_names[i]}'"
            else:
                login_name = ""

            if display_names[i] != None:
                display_name = f" display_name='{display_names[i]}'"
            else:
                display_name = ""

            if default_roles[i] != None:   
                default_role = f" default_role={default_roles[i]}"
            else:
                default_role = ""

            if emails[i] != None:
                email = f" email='{emails[i]}'"
            else:
                email = ""

            sql = f"""CREATE OR REPLACE USER {name} password={password} {login_name} \
                        {display_name} {default_role} {email}"""
            print(sql)

            self.target_cur.execute(sql)
            
            
        drop_user_sql_list = [f"""DROP USER IF EXISTS '{user}';""" for user in names]
        print("created users")
        
        return drop_user_sql_list
    
    
    def warehouses(self):
        """ - Reads warehouses from the source account
            - Creates warehouses in the target account
            - Outputs a list of sql for dropping objects
            - Warehouses created with: name, size
        """

        sql = """show warehouses;"""
        df_wh = pd.read_sql(sql, self.source_conn)

        cur = conn_acct_target.cursor()

        warehouses = df_wh['name'].values.tolist()
        wh_sizes = df_wh['size'].values.tolist()

        wh_list = [ f"""CREATE OR REPLACE warehouse {wh} warehouse_size='{size}' initially_suspended=true;""" \
                   for wh, size in zip(warehouses, wh_sizes)]
        
        [self.target_cur.execute(sql) for sql in wh_list]
        
        drop_wh_list = [f"""DROP WAREHOUSE {wh};""" for wh in warehouses]
        print("created warehouses")
        
        return drop_wh_list
    
    
    def user_role_grants(self):
        """ - Reads grants from the source account
            - Creates role grants for users in the target account
            - Future grants not supported yet
        """

        sql = """select * from snowflake.account_usage.grants_to_users;"""
        df_user_grants = pd.read_sql(sql, self.source_conn)

        roles = df_user_grants['ROLE'].values.tolist()
        users = df_user_grants['GRANTEE_NAME'].values.tolist()
        
        user_role_grant_list = [f"""GRANT ROLE "{role}" TO USER "{user}";""" \
                                for role, user in zip(roles, users)]
        
        [self.target_cur.execute(sql) for sql in user_role_grant_list]
        print("created user -> role grants")
        
        
    def role_role_grants(self):    
        """ - Reads grants from the source account
            - Creates role grants for roles in the target account
            - Future grants not supported yet
        """

        sql = """select * from snowflake.account_usage.grants_to_roles;"""
        df_grants = pd.read_sql(sql, self.source_conn)
        
        # just looking at roles in this step
        df_role_grants = df_grants[df_grants['GRANTED_ON'] == 'ROLE']
        role_sources = df_role_grants['NAME'].values.tolist()
        role_targets =df_role_grants['GRANTEE_NAME'].values.tolist()
        
        role_role_grant_list = [f"""GRANT ROLE "{role_source}" TO ROLE "{role_target}";""" \
                               for role_source, role_target in zip(role_sources, role_targets)]
        
        [self.target_cur.execute(sql) for sql in role_role_grant_list]
        print("created role -> role grants")

        
    def role_object_grants(self):
        """ - Reads grants from the source account
            - Creates object grants for roles in the target account
            - Future grants not supported yet
        """
        
        ## Ingore Account, Integration, User, Role object types
        ## privelige -> object_type -> object_name : to role (grantee) 
        sql = """select * from snowflake.account_usage.grants_to_roles;"""
        df_grants = pd.read_sql(sql, self.source_conn)
        
        supported_object_types = ['WAREHOUSE', 'DATABASE', 'SCHEMA', 'TABLE', 'VIEW']
        df_obj_grants = df_grants[df_grants['GRANTED_ON'].isin(supported_object_types)]
        
        # filter out any snowflake objects
        snowflake_obj_list = ['SNOWFLAKE_SAMPLE_DATA', 'SNOWFLAKE']
        df_obj_grants = df_obj_grants[~df_obj_grants['NAME'].isin(snowflake_obj_list)]
        
        privileges = df_obj_grants['PRIVILEGE'].values.tolist()
        object_types = df_obj_grants['GRANTED_ON'].values.tolist()
        object_names = df_obj_grants['NAME'].values.tolist()
        object_name_schemas = df_obj_grants['TABLE_SCHEMA'].values.tolist()
        object_name_dbs = df_obj_grants['TABLE_CATALOG'].values.tolist()
        grantee_roles = df_obj_grants['GRANTEE_NAME'].values.tolist()

        for i in range(0, len(df_obj_grants)):  
            privilege = privileges[i]
            object_type = object_types[i]
            object_name = object_names[i]
            grantee_role = grantee_roles[i]
            object_name_schema = object_name_schemas[i]
            object_name_db = object_name_dbs[i]

            # Some objects need a full name/path
            if object_type == 'TABLE' or object_type == 'VIEW':
                full_object_name = f"{object_name_db}.{object_name_schema}.{object_name}"
            elif object_type == 'SCHEMA':
                full_object_name = f"{object_name_db}.{object_name}"
            else:
                full_object_name = object_name

            # some restrictions on granting ownership
            if privilege == 'OWNERSHIP':   
                sql = f"""GRANT {privilege} ON {object_type} {full_object_name} TO ROLE  {grantee_role} REVOKE CURRENT GRANTS; """
            else:
                sql = f"""GRANT {privilege} ON {object_type} {full_object_name} TO ROLE  {grantee_role}; """
            
            print(sql)
            self.target_cur.execute(sql)
        
        
        print("created role -> object grants")
        
        
    def copy_account(self):
        """ Function to create all objects. Specific object selection not supported yet """

        self.db_drop_sql_list = self.database_objects()
        self.drop_user_sql_list = self.users()
        self.drop_roles_sql_list = self.roles()
        self.drop_wh_list = self.warehouses()
        self.user_role_grants()
        self.role_object_grants()
        
        print("created account objects")
        
    def drop_added_objects(self):
        """ Drops all created objects (needs additional testing). Depends on copy_account being ran """
        sql_drop_list = self.db_drop_sql_list + self.drop_user_sql_list + \
                            self.drop_roles_sql_list + self.drop_wh_list
        
        [self.target_cur.execute(sql) for sql in sql_drop_list]
        print("dropped objects")
        

# if you wan't to run the script direclty either use a config file called 'snowflake.config' 
# or change the config file name

if __name__== "__main__":
    # test running from config file called snowflake.config
    sf_conn = transcribe_snowflake_account('snowflake.config')
    sf_conn.copy_account()
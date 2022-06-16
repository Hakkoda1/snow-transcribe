from transcribe import transcribe_snowflake_account


# initialize connections
sf_transcribe = transcribe_snowflake_account('example_creds.config',
                                       source_config_name='snowflake_source_account', 
                                       target_config_name='snowflake_target_account', 
                                       conn_type_source = 'private_key', 
                                       conn_type_target = 'private_key',
                                       db_ignore_list = [""],
                                       return_sql = True)


# to copy all available objects 
# (note this takes a while, for the hakkoda account it took >5 minutes)
sf_transcribe.copy_account()

# drop created objects
sf_transcribe.drop_objects(objects = 'all')


# you can also copy over object categories like database objects
sf_transcribe.database_objects()

# Other objects categories are available
sf_transcribe.roles()
sf_transcribe.warehouses()
sf_transcribe.user_role_grants()
sf_transcribe.role_role_grants()
sf_transcribe.role_object_grants()



# to drop only the database objects:
sf_transcribe.drop_objects(objects = 'databases')
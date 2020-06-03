import configparser

config = configparser.ConfigParser()
config.read_file(open('./settings.ini'))

def get_app_token():
    return config.get('API','app_token')

def get_database_connection_url():
    return config.get('DATABASE','connection_url')

def get_database_connection_parameters():
    return (
        config.get('DATABASE','host'),
        config.get('DATABASE','database'),
        config.get('DATABASE','user'),
        config.get('DATABASE','password')
    )

def get_database_name():
    return config.get('DATABASE','database')

def get_database_user():
    return config.get('DATABASE','user')

def get_database_table():
    return config.get('DATABASE','table')

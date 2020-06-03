from requests import get

def get_current_ip():
    return get('https://api.ipify.org').text
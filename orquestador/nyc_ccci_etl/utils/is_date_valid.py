from datetime import datetime
def is_date_valid(y, m, d):
    try:
        date = datetime(int(y),int(m),int(d))
        if date > datetime.now():
            return False
    except:
        return False
    return True
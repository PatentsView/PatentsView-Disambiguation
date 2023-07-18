import os
from datetime import datetime


def get_file_modified_time(file_path):
    try:
        mtime = os.path.getmtime(file_path)
    except OSError:
        mtime = 0
    last_modified_date = datetime.fromtimestamp(mtime)
    return last_modified_date

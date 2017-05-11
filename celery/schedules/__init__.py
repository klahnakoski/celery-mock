from mo_dots import Null
from mo_logs import Log


def crontab(*args, **kwargs):
    Log.note("called crontab with parameters {{param}}", param={"args": args, "kwargs": kwargs})
    return Null

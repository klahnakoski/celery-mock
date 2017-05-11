from mo_logs import Log


def get_task_logger(name):
    return Logger(name)


class Logger(object):
    def __init__(self, name):
        self.name = name

    def info(self, description, *args):
        Log.note(description % args, stack_depth=1)

    def debug(self, description, *args):
        Log.note(description % args, stack_depth=1)

    def exception(self, description, *args):
        Log.warning(description % args, stack_depth=1)

# encoding: utf-8
#
# Derived from celery.worker.job.py  May 2017
#
# Celery is licensed under The BSD License (3 Clause, also known as
# the new BSD license).  The license is an OSI approved Open Source
# license and is GPL-compatible(1).
#
# The license text can also be found here:
# http://www.opensource.org/licenses/BSD-3-Clause
#
from copy import deepcopy
from mo_dots import Data, wrap, set_default, unwrap, Null
from mo_json import value2json, json2value
from mo_logs import Log, Except
from mo_threads import Queue, Thread, Lock
from mo_times import Date

from celery import states
from celery.result import AsyncResult
from celery.signals import worker_process_init


class Celery(object):

    _fixups = None
    _pool = None

    def __init__(
        self,
        name,
        broker=None,
        include=None,
        **kwargs
    ):
        self.Task = MethodCaller

        self.name = name
        self.request_queue = Queue(name=name+" requests")
        self.response_queue = Queue(name=name+" responses")
        self.kwargs = kwargs
        self.include = include
        self.broker = broker
        self._config = {}
        self._tasks = {}
        self.on_init()
        self.response_worker = Thread.run("response worker", self._response_worker)
        self.responses = {}
        self.responses_lock = Lock()
        self.id_lock = Lock()
        self.next_id = 1
        self.worker = Worker(self.request_queue, self.response_queue, celery=self)

    def _response_worker(self, please_stop):
        while not please_stop:
            try:
                encoded_mail = self.response_queue.pop(till=please_stop)
                mail = json2value(encoded_mail)
                Log.note("got response for {{id}}", id=mail.request.id)
                with self.responses_lock:
                    try:
                        async_response = self.responses[mail.request.id]
                        async_response.mail = set_default(mail, async_response.mail)
                        if mail.status in states.READY_STATES:
                            async_response._ready.go()
                    except Exception as e:
                        Log.warning("not expected", cause=e)


            except Exception as e:
                Log.warning("not expected", cause=e)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        self.response_worker.stop()

    def on_init(self):
        """Optional callback called at init."""
        pass

    def start(self, argv=None):
        pass

    @property
    def conf(self):
        return self._config

    def task(self, **opts):
        opts = wrap(opts)
        this = self

        def dec(fun):
            # GET THE PARAMETER NAMES FOR args
            arg_names = fun.func_code.co_varnames[:fun.func_code.co_argcount]
            if arg_names and arg_names[0] == 'self':
                arg_names = arg_names[1:]
            this._tasks[opts.name] = fun

            def async(args, kwargs=None, *_args, **_kwargs):
                kwargs = set_default(kwargs, dict(zip(arg_names, args)))

                with self.id_lock:
                    id = self.next_id
                    self.next_id += 1

                mail = deepcopy(Data(
                    status=states.PENDING,
                    caller={
                        # "stack": extract_stack(1)
                    },
                    sender=set_default(_kwargs, opts),
                    message=kwargs,
                    request=set_default({"id": id})
                ))

                output = AsyncResult(id, mail=mail, app=self)
                with self.responses_lock:
                    self.responses[id] = output
                self.request_queue.add(value2json(mail))
                Log.note("Added {{id}} ({{name}}) to request queue\n{{request}}", id=id, name=opts.name, request=mail)
                return output

            def send_message(*args, **kwargs):
                return async(args, kwargs)

            def revoke(terminate=True, signal='SIGINT'):
                pass

            setattr(send_message, "delay", send_message)
            setattr(send_message, "revoke", revoke)
            setattr(send_message, "apply_async", async)

            return send_message
        return dec

    def get_result(self, id):
        with self.responses_lock:
            response, self.responses[id] = self.responses[id], None
        response._ready.wait()
        if response.status in states.EXCEPTION_STATES:
            Log.error("bad response", cause=response.mail.result)
        else:
            return response.mail.result


class MethodCaller(object):

    def __call__(self, _fun, *args, **kwargs):
        """
        A function that calls a function
        Unfortunatly required, so applications that use global context 
        variables can setup/teardown that global context for the function 
        :param fun: 
        :param args: 
        :param kwargs: 
        :return: 
        """
        try:
            return _fun(*args, **kwargs)
        except Exception as e:
            Log.error("Method execution failure", cause=e)


class Worker(object):

    def __init__(self, request_queue, response_queue, celery):
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.celery = celery
        self.bound_objects ={}

        self.thread = Thread.run("worker", self.work_loop)
        self.call = None
        self.dummy = None

    def _status_update(self, mail, status, more=Null):
        """
        UPDATE MESSAGE ID STATUS FOR PTHER PROCESSES TO KNOW
        :param mail: 
        :param status: 
        :return: 
        """
        mail.status = status
        if mail.sender.track_started:
            self.response_queue.add(value2json(set_default({"request": {"id": mail.request.id}, "status": status}, more)))

    def work_loop(self, please_stop):
        this = self
        connected = False
        work_list = {}
        work_list_lock = Lock()

        def process_task(mail, please_stop=None):
            try:
                if not this.call:
                    this.call = this.celery.Task.__call__
                    this.dummy = this.celery.Task()

                name = mail.sender.name
                args = (mail,) if mail.sender.bind else tuple()

                this._status_update(mail, states.STARTED, {"response": {"start_time": Date.now().format()}})
                fun = this.celery._tasks[name]
                mail.result = this.call(this.dummy, fun, *args, **unwrap(mail.message))
                mail.status = states.SUCCESS
            except Exception as e:
                mail.result = Except.wrap(e)
                mail.status = states.FAILURE
                # mail = wrap({"request": {"id": mail.request.id}, "sender": {"name": "mail.sender.name"}})
                Log.warning("worker failed to process {{mail}}", mail=mail, cause=e)

            mail.response.end_time = Date.now().format()
            if isinstance(mail.result, Exception):
                mail.result = Except.wrap(mail.result)
            mail.receiver.thread = None

            Log.note("Add {{id}} ({{name}}) to response queue\n{{result}}", id=mail.request.id, name=mail.sender.name, result=mail)
            this.response_queue.add(value2json(mail))
            with work_list_lock:
                del work_list[mail.request.id]

        while not please_stop:
            try:
                _mail = json2value(self.request_queue.pop(till=please_stop))
            except Exception as e:
                Log.warning("Could not pop work of request queue", cause=e)
                continue

            # MUST WAIT BEFORE TRYING TO CALL THE worker_process_inits
            if not connected:
                connected = True
                for r in worker_process_init.registered:
                    r()

            Log.note("Got {{id}} ({{name}}) from request queue", id=_mail.request.id, name=_mail.sender.name)
            with work_list_lock:
                work_list[_mail.request.id] = _mail
            # _mail.receiver.thread = Thread.run(_mail.sender.name, process_task, _mail)
            process_task(_mail)

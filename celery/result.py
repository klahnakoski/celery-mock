# -*- coding: utf-8 -*-
"""
    celery.result
    ~~~~~~~~~~~~~

    Task results/state and groups of results.

"""
from __future__ import absolute_import

from collections import deque, Mapping
from copy import copy

from mo_dots import Data, unwrap, coalesce
from mo_json import value2json
from mo_threads import Signal, Lock

from celery import states
from celery.exceptions import IncompleteStream
from celery.states import PROPAGATE_STATES


class AsyncResult(object):
    """
    Query task state.
    """

    all_results = {}
    all_results_lock = Lock()
    mail = None

    def __new__(
        cls,
        id,
        mail=None,
        app=None
    ):
        id = int(id)
        with AsyncResult.all_results_lock:
            output = AsyncResult.all_results.get(id)
            if not output:
                output = AsyncResult.all_results[id] = object.__new__(cls)
            if mail:
                output.mail = mail
            return output

    def __init__(
        self,
        id,
        mail=None,
        app=None
    ):
        self.id = int(id)
        self.mail = coalesce(self.mail, mail)
        self.app = app
        self._cache = None
        self._ready = Signal()

    def as_tuple(self):
        parent = self.parent
        return (self.id, parent and parent.as_tuple()), None
    serializable = as_tuple   # XXX compat

    def forget(self):
        """Forget about (and possibly remove the result of) this task."""
        self._cache = None
        self.backend.forget(self.id)

    def revoke(
        self,
        connection=None,
        terminate=False,
        signal=None,
        wait=False,
        timeout=None
    ):
        self.app.response_queue.add(value2json({"request": {"id": self.id}, "status": states.REVOKED}))

    def get(self, timeout=None, propagate=True, interval=0.5,
            no_ack=True, follow_parents=True):
        """Wait until task is ready, and return its result.

        .. warning::

           Waiting for tasks within a task may lead to deadlocks.
           Please read :ref:`task-synchronous-subtasks`.

        :keyword timeout: How long to wait, in seconds, before the
                          operation times out.
        :keyword propagate: Re-raise exception if the task failed.
        :keyword interval: Time to wait (in seconds) before retrying to
           retrieve the result.  Note that this does not have any effect
           when using the amqp result store backend, as it does not
           use polling.
        :keyword no_ack: Enable amqp no ack (automatically acknowledge
            message).  If this is :const:`False` then the message will
            **not be acked**.
        :keyword follow_parents: Reraise any exception raised by parent task.

        :raises celery.exceptions.TimeoutError: if `timeout` is not
            :const:`None` and the result does not arrive within `timeout`
            seconds.

        If the remote call raised an exception then that exception will
        be re-raised.

        """

        self._ready.wait()
        on_interval = None
        if follow_parents and propagate and self.parent:
            on_interval = self._maybe_reraise_parent_error
            on_interval()

        if self._cache:
            if propagate:
                self.maybe_reraise()
            return self.result

        meta = self.backend.wait_for(
            self.id, timeout=timeout,
            interval=interval,
            on_interval=on_interval,
            no_ack=no_ack,
        )
        if meta:
            self._maybe_set_cache(meta)
            status = meta['status']
            if status in PROPAGATE_STATES and propagate:
                raise meta['result']
            return meta['result']
    wait = get  # deprecated alias to :meth:`get`.

    def _maybe_reraise_parent_error(self):
        for node in reversed(list(self._parents())):
            node.maybe_reraise()

    def _parents(self):
        node = self.parent
        while node:
            yield node
            node = node.parent

    def get_leaf(self):
        value = None
        for _, R in self.iterdeps():
            value = R.get()
        return value

    def iterdeps(self, intermediate=False):
        stack = deque([(None, self)])

        while stack:
            parent, node = stack.popleft()
            yield parent, node
            if node.ready():
                stack.extend((node, child) for child in node.children or [])
            else:
                if not intermediate:
                    raise IncompleteStream()

    def ready(self):
        """Returns :const:`True` if the task has been executed.

        If the task is still running, pending, or is waiting
        for retry then :const:`False` is returned.

        """
        return bool(self._ready)

    def successful(self):
        """Returns :const:`True` if the task executed successfully."""
        return self.state == states.SUCCESS

    def failed(self):
        """Returns :const:`True` if the task failed."""
        return self.state == states.FAILURE

    def maybe_reraise(self):
        if self.state in states.PROPAGATE_STATES:
            raise self.result

    def __str__(self):
        """`str(self) -> self.id`"""
        return str(self.id)

    def __hash__(self):
        """`hash(self) -> hash(self.id)`"""
        return hash(self.id)

    def __repr__(self):
        return '<{0}: {1}>'.format(type(self).__name__, self.id)

    def __eq__(self, other):
        if isinstance(other, AsyncResult):
            return other.id == self.id
        elif isinstance(other, basestring):
            return other == self.id
        return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)

    def __copy__(self):
        return self.__class__(
            self.id, self.backend, self.task_name, self.app, self.parent,
        )

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return self.id, self.backend, self.task_name, None, self.parent

    def __del__(self):
        self._cache = None


    @property
    def supports_native_join(self):
        return self.backend.supports_native_join

    @property
    def children(self):
        return self._get_task_meta().get('children')

    def _maybe_set_cache(self, meta):
        if meta:
            state = meta['status']
            if state == states.SUCCESS or state in states.PROPAGATE_STATES:
                return self._set_cache(meta)
        return meta

    def _get_task_meta(self):
        """
        RETURN REDASH-EXPECTED METADATA
        """
        if not self.mail:
            return {"status": states.PENDING, "result": {}}

        output = Data(copy(self.mail))
        if output.result == None:
            output.result = {}
        if output.status == states.PENDING:
            output.result.start_time = output.response.start_time

        return unwrap(output)

    @property
    def result(self):
        """When the task has been executed, this contains the return value.
        If the task raised an exception, this will be the exception
        instance."""
        return self.mail.result
    info = result

    @property
    def traceback(self):
        """Get the traceback of a failed task."""
        return self._get_task_meta().get('traceback')

    @property
    def state(self):
        return self.mail.status
    status = state

    @property
    def task_id(self):
        """compat alias to :attr:`id`"""
        return self.id

    @task_id.setter  # noqa
    def task_id(self, id):
        self.id = id




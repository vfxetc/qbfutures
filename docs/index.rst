.. _index:

qbfutures
=========

This Python package is a implementation of a `concurrent.futures.Executor <http://docs.python.org/dev/library/concurrent.futures.html>`_ for `PipelineFX's Qube <http://pipelinefx.com/>`_. The API is compatible with the standard ``Executor`` and provides extensions for working with Qube.


Overview
--------

Basics
^^^^^^

Basic usage is exactly the same::

    >>> executor = qbfutures.Executor():
    >>> future = executor.submit(my_function, 1, 2, key="value")
    >>> future.result()
    "Awesome results!"

An extended submit function, :meth:`Executor.submit_ext <qbfutures.Executor.submit_ext>`, allows you to provide more information to Qube about how to handle the job. Anything that would normally be set into a ``qb.Job`` object is viable and will be passed through::

    >>> future = executor.submit(my_function, name="Do something", groups="farm")

Keyword arguments can also be passed to :meth:`Executor.map <qbfutures.Executor.map>`::

    >>> results_iter = executor.map(my_function, range(10), cpus=10)

Finally, keyword arguments to the :class:`~qbfutures.Executor` constructor will be used as defaults on all submitted jobs::


    >>> executor = Executor(cpus=4, group='farm')
    >>> # Submit some jobs, and they will take on the cpus and group above.


Batch Mode
^^^^^^^^^^

Often, logical jobs will be spread into multiple chunks of work. If those are processed individually via :meth:`Executor.submit <qbfutures.Executor.submit>` they will be queued as individual jobs. A batch mode has been added to the API to facilitate grouping multiple function calls into a single Qube job::

    >>> with Executor().batch(name="A set of functions", cpus=4) as batch:
    ...    f1 = batch.submit(a_function, 'input')
    ...    f2 = batch.submit_ext(another_function, name='work name')
    ...    map_iter = batch.map(mapping_function, range(10))
    ...
    >>> f1.results()
    >>> f2.results()
    >>> list(map_iter)

While batch methods will return a :class:`~qbfutures.Future`, they will not be in a valid
state until the batch has been submitted. They will not have job or work IDs,
and iterating over a :func:`~Batch.map` result is undefined.

Since jobs submited via a batch are individual work items, extra keyword
arguments to either :func:`Batch.submit_ext` or :func:`Batch.map` will be
passed through to the ``qb.Work``.


Maya
^^^^

An ``Executor`` subclass exists for use with Maya, which will bootstrap the Maya process, and optionally open a file to work on and set the workspace. It also provides convenience functions for cloning the current environment, and creating a temporary copy of the current file for the other processes to work on.

::

    >>> executor = qbfutures.maya.Executor(clone_environ=True, cpus=4)
    >>> executor.create_tempfile()
    >>> with executor.batch("Get Node Types") as batch:
    ...     for node in cmds.ls(sl=True):
    ...         future = batch.submit(cmds.nodeType, node)
    ...         future.node = node
    ...
    >>> for future in as_completed(batch.futures):
    ...     print future.job_id, future.work_id, future.node, future.result()


API Reference
-------------

Executor
^^^^^^^^

.. autoclass:: qbfutures.Executor
    :members:

Future
^^^^^^

.. autoclass:: qbfutures.Future
    :members:

Batch
^^^^^

.. autoclass:: qbfutures.core.Batch
    :members:

Maya
^^^^

.. autoclass:: qbfutures.maya.Executor
    :members:

Indices and tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


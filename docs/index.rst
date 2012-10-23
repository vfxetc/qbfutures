.. _index:

qbfutures
=========

This Python package is a implementation of a `concurrent.futures.Executor <http://docs.python.org/dev/library/concurrent.futures.html>`_ for `PipelineFX's Qube <http://pipelinefx.com/>`_. The API is compatible with the standard ``Executor`` and provides extensions for working with Qube.


Overview
--------

Basic usage is exactly the same::

    >>> executor = qbfutures.Executor():
    >>> future = executor.submit(my_function, 1, 2, key="value")
    >>> future.result()
    "Awesome results!"

An extended submit function, :meth:`Executor.submit_ext <qbfutures.Executor.submit_ext>`, allows you to provide more information to Qube about how to handle the job. Anything that would normally be set into a ``qb.Job`` object is viable and will be passed through::

    >>> future = executor.submit(my_function, name="Job Name", groups="farm")

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

A :class:`maya.Executor <qbfutures.maya.Executor>` subclass exists for use with Maya, which will bootstrap the Maya process, and optionally open a file to work on and set the workspace. It also provides convenience functions for cloning the current environment, and creating a temporary copy of the current file for the other processes to work on.

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


Installation
------------

This package depends upon ``concurrent.futures``, which is included with Python 3. For Python 2, the `futures <http://pypi.python.org/pypi/futures>`_ package provides a backport.

Qube must also have access to the custom jobtype; either the ``qbfutures`` type must be copied to where your jobtypes are stored, or the ``types`` directory must be added to the ``worker_template_path`` within the ``qb.conf`` for your workers.


Special Considerations
----------------------

Unlike threads, callables and their arguments must be serialized (via :mod:`pickle`) to be passed to the Qube workers. This places some restrictions upon what can be used. A non-exhaustive list of rules include:

- Callables (functions or classes) must be within the global scope of a module.
- Callables must be uniquely named within that module.
- A callable's module must have a ``__name__`` that is not importable.
- Lambdas are not permissable (since they cannot be pickled).


Within ``__main__``
^^^^^^^^^^^^^^^^^^^

Many of our tools are called via the ``-m`` switch of the python interpreter. In that case, callables within the main module are not unpickleable since their module is named ``__main__`` and then the callable will not be found. As such, you may also pass a string in the form ``package.module:function`` to specify a callable::

    >>> executor.submit('awesome_package.the_tool:qube_handler', *args)


Lambda Workaround
^^^^^^^^^^^^^^^^^

Even though lambdas are not pickleable, we can also acheive the same effect as lambdas by calling ``eval`` and passing in a string of Python source code, and a dictionary for the scope to run that code in::

    >>> executor.submit(eval, 'a + b', dict(a=1, b=2)).result()
    3


Recursion
^^^^^^^^^

Jobs are free to schedule additional jobs, but sometimes this can run away from us and take over all of the worker resources. Therefore, a very conservative recursion limit has been setup;  by default recusion will only be allowed to 4 levels, and the 5th recursive job will fail to schedule.

A ``QBLVL`` variable has been placed into the execution environment to track how deep the current recursion is, with the first job assuming a value of 1.

The recusion limit may be increased by setting a ``QBFUTURES_RECURSION_LIMIT`` variable in the environment.


Indices and tables
------------------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


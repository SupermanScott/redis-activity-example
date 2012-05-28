======================
Activity Redis Example
======================

--------
Overview
--------

Redis powered activity feed with Aggregation on user_id, activity_type and day. Setup as 4 Celeryd tasks new_activity, delete_activity, follow and unfollow.

-------
Install
-------
Download, install and run Redis::
   http://redis.io/download

::

    pip install -r requirements.txt

::

    celeryd

Execute some tasks::

    Python 2.7.3 (default, Apr 20 2012, 22:39:59)
    [GCC 4.6.3] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import time
    >>> import tasks
    >>> tasks.new_activity.delay(1, time.time(), 'picture')
    <AsyncResult: 1cffc586-0442-4824-8c8f-7fc2c72de1ff>
    >>> tasks.new_activity.delay(1, time.time(), 'like')
    <AsyncResult: 20683c9f-676d-4f52-a911-b561f651bb0b>
    >>> tasks.new_activity.delay(1, time.time(), 'picture')
    <AsyncResult: 4e19bdc3-2c19-442a-93f9-065d22bdcdc9>
    >>> tasks.follow_user.delay(1, 2)
    <AsyncResult: c570c9c3-13d8-4688-ae41-0ebbdc337f45>
 

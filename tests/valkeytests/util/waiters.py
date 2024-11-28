"""
Waiters
All the wait functions that are defined here,
wait for a given function to return a value that satisfies the given operator, and
assert that this happens within a timeout.
Currently implemented: equal
Future implemanted: true, not equal, greater than, greater or equal than, less than, less or euqal than
All the function get the following parameters:
args:
func: The function to call
expected_value: The expected value to compare against (default: True for wait_for_true and @wait)
op: The operator to use for comparison default: operator.eq for wait_for_equal
on_failure: A function to call if the wait fails (default: None)
timeout: The maximum time to wait for the function to return the expected value (default: TEST_MAX_WAIT_TIME_SECONDS)
ignore_exception: The exception to ignore (default is None)
"""
import contextlib
import warnings
from functools import wraps
import operator
import time
import os
import traceback
import logging

# The maximum wait time for operations in the tests
TEST_MAX_WAIT_TIME_SECONDS = 90
# Setting higher timeout for asan runs
if os.environ.get('ASAN_BUILD') is not None:
    TEST_MAX_WAIT_TIME_SECONDS = 180

class WaitTimeout(Exception):
    def __init__(self, message):
        super().__init__(message)

class wait(object):
    """
    Decorator to wait_for_true function. This decorator can be used to retry a function until it returns true.
    """
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            wait_for_true(lambda: func(*args, **kwargs), **self.kwargs)
        return func_wrapper

def  _wait_for(func, expected_value=True, op=operator.eq, on_failure=None, timeout=TEST_MAX_WAIT_TIME_SECONDS, ignore_exception=None):
    start_time = time.time()
    while True:
        try:
            return_value = func()
        except Exception as e:
            # check if the exception is the same as ignore_exception
            if ignore_exception is not None and isinstance(e, ignore_exception):
                return_value = e
                logger = logging.getLogger('wait.log').debug(traceback.format_exc())
            else:
                raise e
        else:
            if op(return_value, expected_value):
                return True
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            if on_failure is not None:
                on_failure()
            if op == operator.eq:
                raise WaitTimeout(f"Expected value to be: {expected_value}, actual value is: {return_value}")
            else:
                raise WaitTimeout(f"Expected operation \"{op.__name__}\" on lhs: {return_value} and rhs: {expected_value} to be True")
        # We sleep less at the beginning, and more (max 1 second) to return as soon as possible if the condition is met.
        time.sleep(min(elapsed_time, 1))



def wait_for_equal(func, expected_value,  **kwargs):
    """
    Wait for a given function to return the expected_value, and
    asserts that this happens within a timeout.
    """
    _wait_for(func, expected_value, **kwargs)

def wait_for_true(func, **kwargs):
    """
    Wait for a given function to return True, and
    asserts that this happens within a timeout.
    """
    _wait_for(func, **kwargs)

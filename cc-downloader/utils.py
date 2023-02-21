from botocore.exceptions import ClientError
import time

def exponential_backoff(func, *args, **kwargs):
    """Exponential backoff to deal with request limits"""
    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 4  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            return func(*args, **kwargs)
        except ClientError:
            time.sleep(delay)
            delay += delay_incr
    else:
        raise
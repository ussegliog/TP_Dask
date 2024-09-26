#!/usr/bin/env python
# coding: utf-8

import time
import dask
from dask.distributed import Client

# Define some processing
def double(x):
    time.sleep(1)
    return 2 * x

def inc(x):
    time.sleep(1)
    return x + 1

def is_even(x):
    return not x % 2


def complete_function(x):
    """Define the whole processing
    """

    if is_even(x):
        y = double(x)
    else:
        y = inc(x)

    return y

# // functions
def define_delayed(data):
    """Define the whole processing and add delayed decorator
    """
    results_delayed = []

    for x in data:
        if is_even(x):
            y = dask.delayed(double)(x)

        else:
            y = dask.delayed(inc)(x)

        results_delayed.append(y)

    return results_delayed


# Main program
if __name__ == "__main__":

    # Input data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    with_futures = False

    # Define a client, you can adapt the number of worker and thread per workers
    client = Client(threads_per_worker=1, n_workers=2)

    # // code
    # We iterate through a list of inputs.
    # If that input is even then we want to call inc.
    # If the input is odd then we want to call double
    # is_even is quite fast => has to be made immediately

    # Two ways to estimate total : delayed or futures
    if not with_futures:
        print("Using delayed functions ....")

        results_delayed = define_delayed(data)


        total = dask.delayed(sum)(results_delayed)

        # Visualize and compute results
        total.visualize(rankdir='LR',
                        filename='total_delayed.svg')
        print(total.compute())

    else:
        print("Using futures ....")

        total_futures = client.map(complete_function, data)
        client.gather(total_futures)
        total = dask.delayed(sum)(total_futures)
        print(total.compute())

    # Close our client
    client.close()

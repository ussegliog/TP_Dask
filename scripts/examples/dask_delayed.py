#!/usr/bin/env python
# coding: utf-8


import time
import dask
from dask.distributed import Client

# Define some processing
def inc(x):
    time.sleep(1)
    return x + 1

def dec(x):
    time.sleep(2)
    return x - 1

def add(x, y):
    time.sleep(1)
    return x + y


if __name__ == "__main__":

    # Define a client, you can adapt the number of worker and thread per workers
    client = Client(threads_per_worker=1, n_workers=2)

    # Declare some lists
    results = []
    z_delayed = []

    # Input data
    data = [1, 2, 3]


    # Declare processing as delayed
    for in_val in data:
        x = dask.delayed(inc)(in_val)
        y = dask.delayed(dec)(in_val+1)
        z_delayed.append(dask.delayed(add)(x, y))

    # Look at the task graph for `z`
    try:
        z_delayed[0].visualize(rankdir='LR', filename='one_delayed.svg')
    except RuntimeError:
        print("Missing graphiz to draw dask graph")

    # Compute one processing
    start_time = time.time()
    z = z_delayed[0].compute()
    print("z for data[0] = {} in {} seconds".format(z, (time.time() - start_time)))

    # Compute all processing (above calculation is reused, here)
    start_time = time.time()
    results = dask.compute(*z_delayed)
    print("results = {} in {} seconds".format(results, (time.time() - start_time)))

    # Close our client
    client.close()

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


    # Run processing, call explicitly client
    x = client.map(inc, data)
    y = client.map(dec, [x+1 for x in data])
    z = client.map(add, x, y)

    # processing has started and has returned futures
    print(z)

    # Block until first estimation
    print(z[0].result())

    # Block until all estimations
    results = client.gather(z)
    print(results)

    # Close our client
    client.close()

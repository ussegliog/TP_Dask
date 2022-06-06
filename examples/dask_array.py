#!/usr/bin/env python
# coding: utf-8


import time
import numpy as np
import psutil
import dask
import dask.array as da
from dask.distributed import Client


if __name__ == "__main__":

    # Define a client, you can adapt the number of worker and thread per workers
    client = Client(threads_per_worker=1, n_workers=2)

    # Define some csts
    size_dim = 20000
    chunk_size = 1000
    dask_mode = True

    start_time = time.time()
    if dask_mode:
        x = da.random.normal(10, 0.1, size=(size_dim, size_dim),
                             chunks=(chunk_size, chunk_size))
        y = x.mean(axis=0)[::100]
        mean = y.compute()
    else:
        x = np.random.normal(10, 0.1, size=(size_dim, size_dim))
        mean = x.mean(axis=0)[::100]

    print("mean = {} in {} seconds".format(mean, (time.time() - start_time)))
    print("residual memory : {} GB".\
          format(psutil.Process().memory_info().rss/(1024*1024*1024)))

    # Close our client
    client.close()

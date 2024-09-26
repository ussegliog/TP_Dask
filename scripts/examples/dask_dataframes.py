#!/usr/bin/env python
# coding: utf-8

import dask
import dask.array as da

# Example from : https://examples.dask.org/dataframe.html


if __name__ == "__main__":

    # create a random timeseries
    df = dask.datasets.timeseries()

    print("datatype = {}".format(df.dtypes))
    print("size = {}".format(len(df)))
    print("first elts = {}".format(df.head()))


    print("########## Doing panda operations ##########")

    # Apply some pandas operations
    df2 = df[df.y > 0]
    df3 = df2.groupby("name").x.std()

    # Start calculation
    computed_df = df3.compute()
    type(computed_df)

    # panda ex : number 2
    df4 = df.groupby("name").aggregate({"x": "sum", "y": "max"})
    df4.compute()

    # Time series operations
    df[["x", "y"]].resample("1h").mean().head()

    # computes the rolling 24 hour mean of the data.
    head = df[["x", "y"]].rolling(window="24h").mean().head()

    # Print it
    print("rolling 24 hour : {}".format(head))

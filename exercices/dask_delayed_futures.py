#!/usr/bin/env python
# coding: utf-8

import time

# Define some processing
def double(x):
    time.sleep(1)
    return 2 * x

def inc(x):
    time.sleep(1)
    return x + 1

def is_even(x):
    return not x % 2

# Main program
if __name__ == "__main__":

    # Input data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


    # Sequential code
    # We iterate through a list of inputs.
    # If that input is even then we want to call inc.
    # If the input is odd then we want to call double
    # is_even is quite fast => has to be made immediately
    results = []

    for x in data:
        if is_even(x):
            y = double(x)

        else:
            y = inc(x)

        results.append(y)


    total = sum(results)
    print(total)

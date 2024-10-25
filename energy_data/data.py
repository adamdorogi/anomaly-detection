import asyncio
import time
import math
import random

def _get_energy_value(timestamp, seasonal_period=365, seasonal_amplitude=500, regular_period=7, regular_amplitude=250, noise=500, y_offset=25_000):
    """Calculates the energy value at a timestamp.

    This function procedurally generates a data point given a timestamp, for
    simulation purposes. This procedural function will always return the same
    energy value for the same timestamp, making it useful for getting the current
    energy value, going back in time to look at a data point at a timestamp in the
    past, or predicting any data point in the future.

    This function generates a data point by combining the following 4 functions:
    - A seasonal function: a `sin` function for representing annual cycles, with its
      period being 365 days.
    - A regular function: a `sin` function for representing regular weekly cycles,
      with its period being 7 days.
    - A random function: a gaussian random function to add noise to the data.
    - A constant Y offset.

    Parameters
    ----------
    timestamp : int
        The timestamp value for which to calculate the energy value.
    seasonal_period : int, optional
        The period (in days) for which a seasonal cycle lasts. Defaults to 365 days.
    seasonal_amplitude : int, optional
        The deviation of the highs and lows of the seasonal cycle. Defaults to 500 GWh.
    regular_period : int, optional
        The period (in days) for which a regular cycle lasts. Defaults to 7 days.
    regular_amplitude : int, optional
        The deviation of the highs and lows of the regular cycle. Defaults to 250 GWh.
    noise : int, optional
        Random noise added to the data. Defaults to 500.
    y_offset : int, optional
        The Y offset of the data. Defaults to 25,000 GWh.

    Raises
    ------
    ValueError
        If timestamp is not a valid integer.

    Returns
    -------
    float
        A floating point number representing an energy value in GWh.
    """
    try:
        timestamp = int(timestamp)
    except ValueError:
        raise ValueError("Timestamp is invalid.")

    # Seasonal function
    seasonal_fn = seasonal_amplitude * math.sin(2 * math.pi * timestamp / (seasonal_period * 24 * 60 * 60))

    # Regular function
    regular_fn = regular_amplitude * math.sin(2 * math.pi * timestamp / (regular_period * 24 * 60 * 60))

    # Random function seeded by timestamp, so we will always get the same value for
    # a given timestamp. This allows procedurally getting timestamps at any point in
    # the past, and 'predicting' them at any point in the future.
    random.seed(timestamp)
    random_fn = random.gauss() * noise

    return seasonal_fn + regular_fn + random_fn + y_offset

def get_historical_energy_data(start, end, timestamp_increment=86_400):
    """Returns historical energy values between two timestamps.

    This function calculates and returns energy values between any two timestamps
    procedurally, in increments of `timestamp_increment`. Timestamps may be at any
    point in the past, present or future.

    Parameters
    ----------
    start : int
        The timestamp from which to calculate energy values.
    end : int
        The timestamp to which to calculate energy values.
    timestamp_increment : int, optional
        The value by which the timestamps from `start` should be incremented until
        `end` is reached. Defaults to 86,400 seconds (1 day).

    Raises
    ------
    ValueError
        If timestamp increment is not greater than 0.

    Returns
    -------
    list
        A list of data points between `start` and `end` represented as a list of
        tuples, with each tuple having two elements: an X value (timestamp of the
        data recorded), and a Y value (energy value in GWh).
    """
    if timestamp_increment <= 0:
        raise ValueError("Timestamp increment must be greater than 0.")

    # Round up to the nearest `timestamp_increment` timestamp.
    timestamp = math.ceil(start / timestamp_increment) * timestamp_increment

    # Calculate the number of steps between our starting and ending timestamps.
    steps = math.floor((end - timestamp) / timestamp_increment) + 1
    
    # Build and return list of values.
    return [(timestamp + timestamp_increment * i, _get_energy_value(timestamp + timestamp_increment * i)) for i in range(steps)]

async def generate_energy_data_stream(timestamp_increment=86_400, stream_delay=1):
    """Simulates an energy data stream.

    This generator simulates a continuous real-time data series, yielding data points
    every `stream_delay` seconds. This is done indefinitely. The yielded data points
    are represented as a tuple with two elements: an X value (timestamp in seconds),
    and a Y value (energy value in GWh).

    Parameters
    ----------
    timestamp_increment : int, optional
        The value by which the timestamp in the yielded tuple should be incremented.
        Defaults to 86,400 seconds (1 day).
    stream_delay : int, optional
        The delay (in seconds) between each data point streamed. Defaults to 1 second.

    Raises
    ------
    ValueError
        If timestamp increment, or stream delay is not greater than 0.

    Yields
    ------
    tuple
        A data point represented as a tuple with two elements: an X value (timestamp
        of the data recorded), and a Y value (energy value in GWh).
    """
    if timestamp_increment <= 0:
        raise ValueError("Timestamp increment must be greater than 0.")
    if stream_delay <= 0:
        raise ValueError("Stream delay must be greater than 0.")

    # Round up to the nearest `timestamp_increment` timestamp.
    timestamp = math.ceil(time.time() / timestamp_increment) * timestamp_increment
    count = 0
    while True:
        await asyncio.sleep(stream_delay) # Simulate delay
        yield (timestamp, _get_energy_value(timestamp))
        timestamp += timestamp_increment
        count += 1

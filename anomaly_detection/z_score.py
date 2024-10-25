import time
import math
from collections import deque

async def process_z_score_outliers(get_historical_data, generate_data_stream, window=45, threshold=2.25, timestamp_increment=86_400, stream_delay=1):
    """Detects outliers of the data points at each point in the stream.

    This generator calculates the rolling mean and rolling sample standard deviation
    at each point in the stream. Then, it calculates how far our current data point
    is away from the rolling mean, in amount of standard deviations. The `threshold`
    parameter defines this number of standard deviations, outside of which we should
    flag data points as outliers.

    Since we're doing rolling window calculations, we'll have to look at historical
    data of size `window - 1` (i.e. go back in time `window - 1` steps) in order to
    get the rolling window values at the first streaming point. Otherwise, we would
    have to wait until we stream `window` values to calculate the rolling mean and
    rolling sample standard deviation. But because we get the historical data, our
    rolling mean and rolling sample standard deviation can be calculated at the first
    step/value of the stream.
    
    Also note: the rolling mean and rolling standard deviation are calculated in
    *constant time* at each data point, instead of iterating through all values in
    the current window. This is achieved by keeping track of the values in the current
    window with a queue. We can then easily (in constant time) access the first and last
    values of the queue, which will assist in calculating the rolling mean and sample
    standard deviation if we move the window one step to the right.

    I.e. assume the rolling window of the queue [4, 3, 5, 9], which has a running total
    of 21 and a mean of 5.25. If we were to roll this window to the next point, say,
    [3, 5, 9, 2], instead of iterating through all of these values again to calculate
    the mean again, we can get the new updated mean by subtracting the last value in our
    previous window (4) from the running total (21), and add the new value of our current
    window (2), then divide by the number of elements (4). So, (21-4+2)/4 would be our
    new mean of 4.75, which we calculated in constant time. This same logic applies to
    calculating the standard deviation, although the formula is slightly more complex.

    Parameters
    ----------
    get_historical_data : function
        The function with which we can retrieve historical data, to aid the rolling window
        calculations.
    generate_data_stream : generator
        The generator which yields our 'live' data stream.
    window : int, optional
        The size of the rolling window in which we'll perform rolling window calculations.
        Defaults to 45 steps.
    threshold : float, optional
        The threshould, in standard deviations away from the rolling mean, outside of which
        we should mark data points as outliers/anomalies.
    timestamp_increment : int, optional
        The value by which the timestamp in the data points should be incremented. Defaults
        to 86,400 seconds (1 day).
    stream_delay : int, optional
        The delay (in seconds) between each data point streamed. Defaults to 1 second.

    Raises
    ------
    ValueError
        If rolling window size is not greater than 1.

    Yields
    ------
    tuple
        A data point represented as a tuple with three elements: an X value (timestamp of the
        data recorded), a Y value (energy value in GWh), and a boolean of whether or not this
        value is an outlier.
    """
    if window <= 1:
        raise ValueError("Rolling window must be greater than 1.")

    # Get historical data `window - 1` steps in the past.
    # This way we can get the moving average at the first data point of the stream.
    historial_data_end_date = time.time()
    historical_data_start_date = historial_data_end_date - (window - 1) * timestamp_increment
    historical_energy_data = get_historical_data(historical_data_start_date, historial_data_end_date)

    # Keep track of the running total of the values, and running total of the squared
    # values. This will be used to assist our rolling mean and rolling sample standard
    # deviation calculations in constant time.
    rolling_sum = 0
    rolling_squared_sum = 0

    # Keep track of the values and squared values in our current rolling window, with queues.
    window_values = deque()
    window_squared_values = deque()

    # Get our historical data, going back `window - 1` steps in the past.
    for _, value in historical_energy_data:
        # Sanity check to make sure we're receiving valid historical data points.
        if not isinstance(value, (int, float)) or value < 0:
            print("Invalid value received, discarding...")
            continue

        rolling_sum += value
        window_values.append(value)

        rolling_squared_sum += value ** 2
        window_squared_values.append(value ** 2)

    # Start streaming our 'live' data.
    async for timestamp, value in generate_data_stream(timestamp_increment=timestamp_increment, stream_delay=stream_delay):
        # Sanity check to make sure we're receiving valid data points from the stream.
        if not isinstance(value, (int, float)) or value < 0:
            print("Invalid value received, discarding...")
            continue

        print(f"Data received from stream • Timestamp: {timestamp} • Value: {value}")

        # Keep track of our running totals and rolling values.
        rolling_sum += value
        window_values.append(value)

        rolling_squared_sum += value ** 2
        window_squared_values.append(value ** 2)

        # Calculate our rolling mean and rolling sample standard deviation, and determine
        # whether our current data point is an outlier.
        rolling_mean = rolling_sum / window
        rolling_std = math.sqrt((window * rolling_mean ** 2 - 2 * rolling_mean * rolling_sum + rolling_squared_sum) / (window - 1))
        is_outlier = abs((value - rolling_mean) / rolling_std) > threshold

        yield (timestamp, value, is_outlier)

        # Remove the old values from the rolling window, and update our running totals.
        old_value = window_values.popleft()
        rolling_sum -= old_value

        old_squared_value = window_squared_values.popleft()
        rolling_squared_sum -= old_squared_value

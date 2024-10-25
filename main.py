import asyncio
import matplotlib.pyplot as plt
from collections import deque

from anomaly_detection.z_score import process_z_score_outliers
from energy_data.data import get_historical_energy_data, generate_energy_data_stream

async def main():
    """
    Retrieve the streamed data and whether the values are outliers or not, and
    visualise this data.

    Use queues to keep track of our streamed data points. Appending to queues is
    constant time. Popping is also constant, which comes in handy when we've seen
    enough data in the plot, and want to pop old values.
    """
    # The number of data points to view in the plot.
    view_window = 365

    # Keep track of our streamed data points and outliers.
    timestamps = deque()
    values = deque()
    outliers = deque()
    
    # Initialise plots for our streamed data points and outliers.
    plt.plot(timestamps, values)
    plt.plot(timestamps, outliers, 'ro')

    steps = 0
    async for (timestamp, value, is_outlier) in process_z_score_outliers(get_historical_energy_data, generate_energy_data_stream, stream_delay=0.05):
        print(f"Plotting data • Timestamp: {timestamp} • Value: {value} • Outlier?: {is_outlier}")

        # Append (in constant time) to our values.
        timestamps.append(timestamp)
        values.append(value)
        outliers.append(value if is_outlier else None)

        # Pop old values once we've reached our desired number of data points.
        if steps > view_window:
            timestamps.popleft()
            values.popleft()
            outliers.popleft()

        # Update the chart data with our latest values.
        plt.gca().lines[0].set_data(timestamps, values)
        plt.gca().lines[1].set_data(timestamps, outliers)

        # Keep the data in view.
        plt.gca().relim()
        plt.gca().autoscale_view()

        plt.pause(0.01) # A slight pause to allow the plots to render.

        steps += 1

if __name__ == "__main__":
    asyncio.run(main())

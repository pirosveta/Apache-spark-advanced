package ru.bmstu.hadoop;

public class TotalStatistics {
    private int maxDelay, totalDelayedCancelledFlights = 0, totalFlights = 0, percentDelayedCancelledFlights;

    public TotalStatistics(int delay, int cancelled) {
        maxDelay = delay;
        totalFlights++;
        if (delay > 0 || cancelled == 1) {
            totalDelayedCancelledFlights++;
        }
    }

    public void updateStatistics(TotalStatistics statistics, )
}

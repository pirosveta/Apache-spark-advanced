package ru.bmstu.hadoop;

public class TotalStatistics {
    private int maxDelay, totalDelayedCancelledFlights = 0, totalFlights, percentDelayedCancelledFlights;

    public TotalStatistics(int delay, int cancelled) {
        maxDelay = delay;
        if (delay > 0 || cancelled == 1) {
            totalDelayedCancelledFlights++;
        }
    }
}

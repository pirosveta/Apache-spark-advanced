package ru.bmstu.hadoop;

import scala.Serializable;

public class TotalStatistics implements Serializable {
    private static final int WAS_FLIGHT = 1, NO_FLIGHT = 0, PERCENT = 100;
    private static final double CANCELLED_FLIGHT = 1, NO_DELAY = 0;

    private int totalDelayedCancelledFlights, totalFlights;
    private double maxDelay, percentDelayedCancelledFlights = 0;

    public TotalStatistics(TotalStatistics total) {
        totalDelayedCancelledFlights = total.getTotalDelayedCancelledFlights();
        totalFlights = total.getTotalFlights();
        percentDelayedCancelledFlights = total.getPercentDelayedCancelledFlights();
        maxDelay = total.getMaxDelay();
    }

    public TotalStatistics(SingleStatistics single) {
        maxDelay = single.getDelay();
        totalFlights = WAS_FLIGHT;
        if (single.getDelay() > NO_DELAY || single.getCancelled() == CANCELLED_FLIGHT) {
            totalDelayedCancelledFlights = WAS_FLIGHT;
        }
        else totalDelayedCancelledFlights = NO_FLIGHT;
        this.setPercentDelayedCancelledFlights();
    }

    public double getMaxDelay() {
        return maxDelay;
    }

    public int getTotalDelayedCancelledFlights() {
        return totalDelayedCancelledFlights;
    }

    public int getTotalFlights() {
        return totalFlights;
    }

    public double getPercentDelayedCancelledFlights() {
        return percentDelayedCancelledFlights;
    }

    private void setMaxDelay(double maxDelay) {
        this.maxDelay = maxDelay;
    }

    private void setPercentDelayedCancelledFlights() {
        this.percentDelayedCancelledFlights = (double) this.totalDelayedCancelledFlights * PERCENT / this.totalFlights;
    }

    private void addTotalDelayedCancelledFlights(int addition) {
        this.totalDelayedCancelledFlights += addition;
    }

    private void addTotalFlights(int addition) {
        this.totalFlights += addition;
    }

    public static TotalStatistics updateStatistics(TotalStatistics total, SingleStatistics single) {
        if (total.getMaxDelay() < single.getDelay()) {
            total.setMaxDelay(single.getDelay());
        }
        total.addTotalFlights(WAS_FLIGHT);
        if (single.getDelay() > NO_DELAY || single.getCancelled() == CANCELLED_FLIGHT) {
            total.addTotalDelayedCancelledFlights(WAS_FLIGHT);
        }
        total.setPercentDelayedCancelledFlights();
        return new TotalStatistics(total);
    }

    public static TotalStatistics update(TotalStatistics first, TotalStatistics second) {
        if (first.getMaxDelay() < second.getMaxDelay()) {
            first.setMaxDelay(second.getMaxDelay());
        }
        first.addTotalFlights(second.getTotalFlights());
        first.addTotalDelayedCancelledFlights(second.getTotalDelayedCancelledFlights());
        first.setPercentDelayedCancelledFlights();
        return new TotalStatistics(first);
    }
}

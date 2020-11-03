package ru.bmstu.hadoop;

import scala.Serializable;

public class FilteredStatistics implements Serializable {
    private static final int WAS_FLIGHT = 1, NO_FLIGHT = 0, PERCENT = 100;
    private static final double CANCELLED_FLIGHT = 1, NO_DELAY = 0;

    private int totalDelayedCancelledFlights, totalFlights;
    private double maxDelay, percentDelayedCancelledFlights;

    public FilteredStatistics(Statistics single) {
        maxDelay = single.getDelay();
        totalFlights = WAS_FLIGHT;
        if (single.getDelay() > NO_DELAY || single.getCancelled() == CANCELLED_FLIGHT) {
            totalDelayedCancelledFlights = WAS_FLIGHT;
        }
        else totalDelayedCancelledFlights = NO_FLIGHT;
        this.setPercentDelayedCancelledFlights();
    }

    public FilteredStatistics(FilteredStatistics total) {
        totalDelayedCancelledFlights = total.getTotalDelayedCancelledFlights();
        totalFlights = total.getTotalFlights();
        percentDelayedCancelledFlights = total.getPercentDelayedCancelledFlights();
        maxDelay = total.getMaxDelay();
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

    public static FilteredStatistics updateStatistics(FilteredStatistics total, Statistics single) {
        if (total.getMaxDelay() < single.getDelay()) {
            total.setMaxDelay(single.getDelay());
        }
        total.addTotalFlights(WAS_FLIGHT);
        if (single.getDelay() > NO_DELAY || single.getCancelled() == CANCELLED_FLIGHT) {
            total.addTotalDelayedCancelledFlights(WAS_FLIGHT);
        }
        total.setPercentDelayedCancelledFlights();
        return new FilteredStatistics(total);
    }

    public static FilteredStatistics update(FilteredStatistics first, FilteredStatistics second) {
        if (first.getMaxDelay() < second.getMaxDelay()) {
            first.setMaxDelay(second.getMaxDelay());
        }
        first.addTotalFlights(second.getTotalFlights());
        first.addTotalDelayedCancelledFlights(second.getTotalDelayedCancelledFlights());
        first.setPercentDelayedCancelledFlights();
        return new FilteredStatistics(first);
    }
}

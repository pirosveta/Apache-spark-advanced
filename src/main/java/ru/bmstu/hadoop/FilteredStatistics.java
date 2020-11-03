package ru.bmstu.hadoop;

import scala.Serializable;

public class FilteredStatistics implements Serializable {
    private static final int WAS_FLIGHT = 1, NO_FLIGHT = 0, PERCENT = 100;
    private static final double CANCELLED_FLIGHT = 1, NO_DELAY = 0;

    private int totalDelayedCancelledFlights, totalFlights;
    private double maxDelay, percentDelayedCancelledFlights;

    public FilteredStatistics(Statistics flight) {
        maxDelay = flight.getDelay();
        totalFlights = WAS_FLIGHT;
        if (flight.getDelay() > NO_DELAY || flight.getCancelled() == CANCELLED_FLIGHT) {
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

    private void setMaxDelay(double delay) {
        this.maxDelay = delay;
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

    public static FilteredStatistics updateStatistics(FilteredStatistics total, Statistics flight) {
        if (total.getMaxDelay() < flight.getDelay()) {
            total.setMaxDelay(flight.getDelay());
        }
        total.addTotalFlights(WAS_FLIGHT);
        if (flight.getDelay() > NO_DELAY || flight.getCancelled() == CANCELLED_FLIGHT) {
            total.addTotalDelayedCancelledFlights(WAS_FLIGHT);
        }
        total.setPercentDelayedCancelledFlights();
        return total;
    }

    public static FilteredStatistics update(FilteredStatistics first, FilteredStatistics second) {
        if (first.getMaxDelay() < second.getMaxDelay()) {
            first.setMaxDelay(second.getMaxDelay());
        }
        first.addTotalFlights(second.getTotalFlights());
        first.addTotalDelayedCancelledFlights(second.getTotalDelayedCancelledFlights());
        first.setPercentDelayedCancelledFlights();
        return first;
    }
}

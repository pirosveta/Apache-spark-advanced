package ru.bmstu.hadoop;

public class TotalStatistics {
    private int maxDelay, totalDelayedCancelledFlights, totalFlights;
    private double percentDelayedCancelledFlights = 0;

    public TotalStatistics(SingleStatistics single) {
        maxDelay = single.getDelay();
        totalFlights = 1;
        if (single.getDelay() > 0 || single.getCancelled() == 1) {
            totalDelayedCancelledFlights = 1;
        }
        else totalDelayedCancelledFlights = 0;
        percentDelayedCancelledFlights = (double) totalFlights * 100 / totalDelayedCancelledFlights;
    }

    public int getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(int maxDelay) {
        this.maxDelay = maxDelay;
    }

    public void setPercentDelayedCancelledFlights(int percentDelayedCancelledFlights) {
        this.percentDelayedCancelledFlights = percentDelayedCancelledFlights;
    }

    public void incTotalDelayedCancelledFlights() {
        this.totalDelayedCancelledFlights++;
    }

    public void incTotalFlights() {
        this.totalFlights++;
    }

    public void updateStatistics(TotalStatistics total, SingleStatistics single) {
        if (total.getMaxDelay() < single.getDelay()) {
            total.setMaxDelay(single.getDelay());
        }
        total.incTotalFlights();
        if (single.getDelay() > 0 || single.getCancelled() == 1) {
            total.incTotalDelayedCancelledFlights();
        }
    }

    public void update(TotalStatistics firstTotal, TotalStatistics secondTotal) {
        if (firstTotal.getMaxDelay() < secondTotal.getMaxDelay()) {
            firstTotal.setMaxDelay(secondTotal.getDelay());
        }
        total.incTotalFlights();
        if (single.getDelay() > 0 || single.getCancelled() == 1) {
            total.incTotalDelayedCancelledFlights();
        }
    }
}

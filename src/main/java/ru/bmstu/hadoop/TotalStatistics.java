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
        this.setPercentDelayedCancelledFlights();
    }

    public int getMaxDelay() {
        return maxDelay;
    }

    public int getTotalDelayedCancelledFlights() {
        return totalDelayedCancelledFlights;
    }

    public int getTotalFlights() {
        return totalFlights;
    }

    public void setMaxDelay(int maxDelay) {
        this.maxDelay = maxDelay;
    }

    public void setPercentDelayedCancelledFlights() {
        this.percentDelayedCancelledFlights = (double) this.totalFlights * 100 / this.totalDelayedCancelledFlights;
    }

    public void addTotalDelayedCancelledFlights(int totalDelayedCancelledFlights) {
        this.totalDelayedCancelledFlights += totalDelayedCancelledFlights;
    }

    public void addTotalFlights(int totalFlights) {
        this.totalFlights += totalFlights;
    }

    public TotalStatistics updateStatistics(TotalStatistics total, SingleStatistics single) {
        if (total.getMaxDelay() < single.getDelay()) {
            total.setMaxDelay(single.getDelay());
        }
        total.addTotalFlights(1);
        if (single.getDelay() > 0 || single.getCancelled() == 1) {
            total.addTotalDelayedCancelledFlights(1);
        }
        total.setPercentDelayedCancelledFlights();
        return total;
    }

    public TotalStatistics update(TotalStatistics firstTotal, TotalStatistics secondTotal) {
        if (firstTotal.getMaxDelay() < secondTotal.getMaxDelay()) {
            firstTotal.setMaxDelay(secondTotal.getMaxDelay());
        }
        firstTotal.addTotalFlights(secondTotal.getTotalFlights());
        firstTotal.addTotalDelayedCancelledFlights(secondTotal.getTotalDelayedCancelledFlights());
        firstTotal.setPercentDelayedCancelledFlights();
        return firstTotal;
    }
}

package ru.bmstu.hadoop;

import scala.Serializable;

public class SingleStatistics implements Serializable {
    private final double NOT_CANCELLED_FLIGHT = 0, NO_DELAY = 0;

    private double delay, cancelled;

    public SingleStatistics(String delay, String cancelled) {
        try {
            this.cancelled = Double.parseDouble(cancelled);
            if (this.cancelled == NOT_CANCELLED_FLIGHT) this.delay = Double.parseDouble(delay);
            else this.delay = NO_DELAY;
        }
        catch (NumberFormatException e) {
            System.out.println(e);
        }
    }

    public double getCancelled() {
        return cancelled;
    }

    public double getDelay() {
        return delay;
    }
}

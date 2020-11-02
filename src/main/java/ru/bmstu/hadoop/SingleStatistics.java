package ru.bmstu.hadoop;

import scala.Serializable;

public class SingleStatistics implements Serializable {
    private double delay, cancelled;

    public SingleStatistics(String delay, String cancelled) {
        try {
            this.cancelled = Double.parseDouble(cancelled);
            if (this.cancelled == 0.00) this.delay = Double.parseDouble(delay);
            else this.delay = 0.00;
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

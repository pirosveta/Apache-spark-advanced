package ru.bmstu.hadoop;

import scala.Serializable;

public class SingleStatistics implements Serializable {
    private int cancelled;
    private double delay;

    public SingleStatistics(String delay, String cancelled) {
        try {
            this.cancelled = Integer.parseInt(cancelled);
            if (this.cancelled == 0) this.delay = Double.parseDouble(delay);
            else this.delay = 0;
        }
        catch (NumberFormatException e) {
            System.out.println(e);
        }
    }

    public int getCancelled() {
        return cancelled;
    }

    public double getDelay() {
        return delay;
    }
}

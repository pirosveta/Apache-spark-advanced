package ru.bmstu.hadoop;

public class SingleStatistics {
    private int delay, cancelled;

    public SingleStatistics(String delay, String cancelled) {
        try {
            this.delay = Integer.parseInt(delay);
            this.cancelled = Integer.parseInt(cancelled);
        }
        catch (NumberFormatException e) {
            System.out.println(e);
        }
    }
}

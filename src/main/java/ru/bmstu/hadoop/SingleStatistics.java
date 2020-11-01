package ru.bmstu.hadoop;

public class Statistics {
    private int delay, cancelled;

    public Statistics(String delay, String cancelled) {
        try {
            this.delay = Integer.parseInt(delay);
            this.cancelled = Integer.parseInt(cancelled);
        }
        catch (NumberFormatException e) {
            System.out.println(e);
        }
    }
}

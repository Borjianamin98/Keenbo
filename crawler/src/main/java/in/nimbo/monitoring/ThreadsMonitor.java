package in.nimbo.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import java.util.List;

public class ThreadsMonitor implements Runnable {
    private List<Thread> threads;

    private Histogram waitingThreadsHistogram;
    private Histogram activeThreadsHistogram;
    private Histogram terminatedThreadsHistogram;

    public ThreadsMonitor(List<Thread> threads) {
        this.threads = threads;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        waitingThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "waitingThreads"));
        activeThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "activeThreads"));
        terminatedThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "terminatedThreads"));
    }

    @Override
    public void run() {
        int newActiveThreads = 0;
        int newWaitingThreads = 0;
        int newTerminatedThreads = 0;
        for (Thread thread : threads) {
            Thread.State state = thread.getState();
            if (state.equals(Thread.State.RUNNABLE)) {
                newActiveThreads++;
            } else if (state.equals(Thread.State.BLOCKED) || state.equals(Thread.State.TIMED_WAITING) || state.equals(Thread.State.WAITING)) {
                newWaitingThreads++;
            } else if (state.equals(Thread.State.TERMINATED)) {
                newTerminatedThreads++;
            }
        }
        activeThreadsHistogram.update(newActiveThreads);
        waitingThreadsHistogram.update(newWaitingThreads);
        terminatedThreadsHistogram.update(newTerminatedThreads);
    }
}

package in.nimbo.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadsMonitor implements Runnable {
    private ThreadGroup threadGroup;
    private AtomicInteger waitingThreads = new AtomicInteger(0);
    private AtomicInteger terminatedThreads = new AtomicInteger(0);
    private AtomicInteger activeThreads = new AtomicInteger(0);
    private AtomicInteger totalThreads = new AtomicInteger(0);

    private Histogram waitingThreadsHistogram;
    private Histogram activeThreadsHistogram;
    private Histogram terminatedThreadsHistogram;
    private Histogram totalThreadsHistogram;

    public ThreadsMonitor(ThreadGroup threadGroup) {
        this.threadGroup = threadGroup;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        waitingThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "waitingThreads"));
        activeThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "activeThreads"));
        terminatedThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "terminatedThreads"));
        totalThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "totalThreads"));
    }

    @Override
    public void run() {
        int newActiveThreads = 0;
        int newWaitingThreads = 0;
        int newTerminatedThreads = 0;
        int newTotalThreads = 0;
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thread : threadSet) {
            if (thread.getThreadGroup().equals(threadGroup) && thread.getName().equals(threadGroup.getName())) {
                newTotalThreads++;
                Thread.State state = thread.getState();
                if (state.equals(Thread.State.RUNNABLE)) {
                    newActiveThreads++;
                } else if (state.equals(Thread.State.BLOCKED) || state.equals(Thread.State.TIMED_WAITING) || state.equals(Thread.State.WAITING)) {
                    newWaitingThreads++;
                } else if (state.equals(Thread.State.TERMINATED)) {
                    newTerminatedThreads++;
                }
            }
        }
        activeThreadsHistogram.update(newActiveThreads);
        waitingThreadsHistogram.update(newWaitingThreads);
        terminatedThreadsHistogram.update(newTerminatedThreads);
        totalThreadsHistogram.update(newTotalThreads);
    }
}

package in.nimbo.monitoring;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadsMonitor implements Runnable {
    private ThreadGroup threadGroup;
    private AtomicInteger waitingThreads = new AtomicInteger(0);
    private AtomicInteger terminatedThreads = new AtomicInteger(0);
    private AtomicInteger activeThreads = new AtomicInteger(0);
    private AtomicInteger totalThreads = new AtomicInteger(0);

    public ThreadsMonitor(ThreadGroup threadGroup) {
        this.threadGroup = threadGroup;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(ThreadsMonitor.class, "waitingThreadsGauge"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return waitingThreads.get();
                    }
                });
        metricRegistry.register(MetricRegistry.name(ThreadsMonitor.class, "activeThreadsGauge"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return activeThreads.get();
                    }
                });
        metricRegistry.register(MetricRegistry.name(ThreadsMonitor.class, "terminatedThreadsGauge"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return totalThreads.get();
                    }
                });
        metricRegistry.register(MetricRegistry.name(ThreadsMonitor.class, "totalThreadsGauge"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return totalThreads.get();
                    }
                });
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
        activeThreads.set(newActiveThreads);
        waitingThreads.set(newWaitingThreads);
        terminatedThreads.set(newTerminatedThreads);
        totalThreads.set(newTotalThreads);
    }
}

package in.nimbo.monitoring;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadsMonitor implements Runnable {
    private ThreadPoolExecutor threadPoolExecutor;
    private ThreadGroup threadGroup;
    private int waitingThreads = 0;
    private int activeThreads = 0;

    public ThreadsMonitor(ThreadPoolExecutor threadPoolExecutor, ThreadGroup threadGroup) {
        this.threadPoolExecutor = threadPoolExecutor;
        this.threadGroup = threadGroup;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(ThreadsMonitor.class, "waitingThreadsGauge"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return getWaitingThreads();
                    }
                });
        metricRegistry.register(MetricRegistry.name(ThreadsMonitor.class, "runningThreadsGauge"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return getActiveThreads();
                    }
                });
    }

    private int getWaitingThreads() {
        return waitingThreads;
    }

    private int getActiveThreads() {
        return activeThreads;
    }

    @Override
    public void run() {
        activeThreads = 0;
        waitingThreads = 0;
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread t : threadSet) {
            if (t.getThreadGroup().equals(threadGroup)) {
                Thread.State state = t.getState();
                if (state.equals(Thread.State.RUNNABLE)) {
                    activeThreads++;
                }
                else if (state.equals(Thread.State.BLOCKED) || state.equals(Thread.State.TIMED_WAITING) || state.equals(Thread.State.WAITING)){
                    waitingThreads++;
                }
            }
        }
    }
}

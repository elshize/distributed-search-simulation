package edu.nyu.tandon.dss.entity;

import co.paralleluniverse.fibers.SuspendExecution;
import desmoj.core.simulator.*;

import java.util.concurrent.TimeUnit;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Query extends Entity {
    private int queryId;
    private int requestId;
    private TimeInstant submitted;

    public Query(Model model, String name, int queryId, int requestId) {
        super(model, name, true);
        this.queryId = queryId;
        this.requestId = requestId;
        this.submitted = model.presentTime();
    }

    public int getQueryId() {
        return queryId;
    }

    public int getRequestId() {
        return requestId;
    }

    public TimeSpan latency() {
        long end = getModel().presentTime().getTimeRounded(TimeUnit.NANOSECONDS);
        long start = submitted.getTimeRounded(TimeUnit.NANOSECONDS);
        return new TimeSpan(end - start, TimeUnit.NANOSECONDS);
    }
}

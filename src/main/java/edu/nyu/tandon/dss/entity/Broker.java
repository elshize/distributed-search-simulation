package edu.nyu.tandon.dss.entity;

import desmoj.core.simulator.*;
import edu.nyu.tandon.dss.DispatchingStrategy;
import edu.nyu.tandon.dss.DistributedSearchSimulation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Broker extends Entity {
    private DistributedSearchSimulation model;

    protected DispatchingStrategy dispatchingStrategy;

    protected Queue<Request> responses;
    protected Map<Integer, Integer> counts;

    public Broker(Model owner, String name, DispatchingStrategy dispatchingStrategy) {
        super(owner, name, true);
        model = (DistributedSearchSimulation)owner;
        counts = new HashMap<>();
        responses = new Queue<Request>(model,
                String.format("Responses Queue (%s)", name), true, true);
        this.dispatchingStrategy = dispatchingStrategy;
    }

    public TimeSpan selectionTime(Query query) {
        // TODO: parametrize
        return new TimeSpan(1, TimeUnit.MILLISECONDS);
    }

    public TimeSpan mergeTime(int shards) {
        // TODO: parametrize
        return new TimeSpan(1, TimeUnit.MILLISECONDS);
    }

    public void initRequestCounter(Query query) {
        counts.put(query.getRequestId(), 0);
    }

    public int requestCount(Query query) {
        //System.out.println(counts.get(query.getRequestId()));
        return counts.get(query.getRequestId());
    }

    public void setRequestCount(Query query, int count) {
        counts.put(query.getRequestId(), count);
    }

    public void clearRequestCount(Query query) {
        counts.remove(query.getRequestId());
    }

    public void queueQueryResponse(Request request) {
        responses.insert(request);
    }

    public Request requestQueryResponse() {
        if (responses.isEmpty()) return null;
        Request nextResponse = responses.first();
        responses.remove(nextResponse);
        return nextResponse;
    }

    public Node selectNode(Request request) {
        return dispatchingStrategy.selectNode(this, request);
    }

    public void makeIdle() {
        model.queueBroker(this);
    }
}

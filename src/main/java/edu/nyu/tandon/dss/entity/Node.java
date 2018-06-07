package edu.nyu.tandon.dss.entity;

import desmoj.core.simulator.*;

import java.util.Set;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Node extends Entity {
    private Set<Integer> shards;

    protected Queue<Request> dispatchedRequests;
    protected Queue<Request> acceptedRequests;
    protected Queue<CPU> idleCPUs;
    protected boolean idle;

    public Node(Model owner, String name, Set<Integer> shards, int numCPUs) {
        super(owner, name, true);
        this.shards = shards;
        dispatchedRequests = new Queue<Request>(owner,
                String.format("Dispatched Requests Queue (%s)", name), true, true);
        acceptedRequests = new Queue<Request>(owner,
                String.format("Accepted Requests Queue (%s)", name), true, true);
        idleCPUs = new Queue<CPU>(owner,
                String.format("Idle CPUs Queue (%s)", name), true, true);
        idle = true;
        for (int idx = 0; idx < numCPUs; idx++) {
            CPU cpu = new CPU(owner, String.format("CPU %d (%s)", idx, getName()), this);
            idleCPUs.insert(cpu);
        }
    }

    public boolean containsShard(int shard) {
        return shards.contains(shard);
    }

    public boolean isIdle() {
        return idle;
    }

    public void setIdle() {
        this.idle = true;
    }

    public void setIdle(boolean idle) {
        this.idle = idle;
    }

    public void queueDispatchedRequest(Request request) {
        dispatchedRequests.insert(request);
    }

    public void queueAcceptedRequest(Request request) {
        acceptedRequests.insert(request);
    }

    public CPU requestCPU() {
        if (idleCPUs.isEmpty()) return null;
        CPU cpu = idleCPUs.first();
        idleCPUs.remove(cpu);
        return cpu;
    }

    public Request requestDispatchedRequest() {
        if (dispatchedRequests.isEmpty()) return null;
        Request nextRequest = dispatchedRequests.first();
        dispatchedRequests.remove(nextRequest);
        return nextRequest;
    }

    public Request requestAcceptedRequest() {
        if (acceptedRequests.isEmpty()) return null;
        Request request = acceptedRequests.first();
        acceptedRequests.remove(request);
        return request;
    }

}

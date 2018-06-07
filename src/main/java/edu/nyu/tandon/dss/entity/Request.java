package edu.nyu.tandon.dss.entity;

import desmoj.core.simulator.Entity;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Request extends Entity {
    private Broker broker;
    private Query query;
    private int shard;
    private int buckets;
    private int selectedShards;

    public Request(Broker broker, Query query, int shard, int buckets, int selectedShards) {
        super(query.getModel(), String.format("Request %d (%s)", query.getRequestId(), query.getName()),
                query.traceIsOn());
        this.broker = broker;
        this.query = query;
        this.shard = shard;
        this.buckets = buckets;
        this.selectedShards = selectedShards;
    }

    public int getId() {
        return query.getRequestId();
    }

    public Query getQuery() {
        return query;
    }

    public int getShard() {
        return shard;
    }

    public int getBuckets() {
        return buckets;
    }

    public Broker getBroker() {
        return broker;
    }

    public int getSelectedShards() {
        return selectedShards;
    }

}

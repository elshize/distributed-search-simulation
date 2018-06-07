package edu.nyu.tandon.dss.event;

import desmoj.core.simulator.EventOf2Entities;
import desmoj.core.simulator.Model;
import desmoj.core.simulator.TimeSpan;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Query;
import edu.nyu.tandon.dss.entity.Request;

import java.util.concurrent.TimeUnit;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class BrokerAcceptResponseEvent extends EventOf2Entities<Broker, Request> {
    private DistributedSearchSimulation model;

    public BrokerAcceptResponseEvent(Model model, String s, boolean b) {
        super(model, s, b);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public void eventRoutine(Broker broker, Request request) {
        int count = broker.requestCount(request.getQuery()) + 1;
        TimeSpan time = new TimeSpan(0);
        if (count == request.getSelectedShards()) {
            broker.clearRequestCount(request.getQuery());
            time = broker.mergeTime(count);
            model.queriesFinished.update();
            TimeSpan latency = request.getQuery().latency();
            model.latencies.update(latency);
            sendTraceNote(String.format("Request %d (Query %d) finished in %f ms",
                    request.getId(), request.getQuery().getQueryId(),
                    latency.getTimeAsDouble(TimeUnit.MILLISECONDS)));
        } else { broker.setRequestCount(request.getQuery(), count); }

        Request nextResponse;
        Query nextQuery = model.requestQuery();
        if (nextQuery != null) { scheduleBrokerAcceptQuery(broker, nextQuery, time);}
        else if ((nextResponse = broker.requestQueryResponse()) != null) {
            schedule(broker, nextResponse, time);
        } else { broker.makeIdle(); }
    }

    private void scheduleBrokerAcceptQuery(Broker broker, Query query, TimeSpan time) {
        BrokerAcceptQueryEvent event = new BrokerAcceptQueryEvent(
                model, "BrokerAcceptQuery", true);
        event.schedule(broker, query, time);
    }
}

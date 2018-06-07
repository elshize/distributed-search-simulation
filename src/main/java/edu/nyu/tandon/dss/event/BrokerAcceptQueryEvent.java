package edu.nyu.tandon.dss.event;

import desmoj.core.simulator.EventOf2Entities;
import desmoj.core.simulator.Model;
import desmoj.core.simulator.TimeSpan;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Query;
import edu.nyu.tandon.dss.entity.Request;
import edu.nyu.tandon.dss.entity.RequestList;

import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class BrokerAcceptQueryEvent extends EventOf2Entities<Broker, Query> {
    private DistributedSearchSimulation model;

    public BrokerAcceptQueryEvent(Model model, String s, boolean b) {
        super(model, s, b);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public void eventRoutine(Broker broker, Query query) {
        List<Request> requests = model.select(broker, query);
        TimeSpan time = broker.selectionTime(query);
        scheduleDispatch(broker, requests, time);
        broker.initRequestCounter(query);

        Query nextQuery;
        Request response = broker.requestQueryResponse();
        if (response != null) {
            scheduleAcceptResponse(broker, response, time);
        } else if ((nextQuery = model.requestQuery()) != null) {
            schedule(broker, nextQuery, time);
        } else {
            model.queueBroker(broker);
        }
    }

    private void scheduleDispatch(Broker broker, List<Request> requests, TimeSpan time) {
        BrokerDispatchEvent event = new BrokerDispatchEvent(model, "BrokerDispatch", true);
        event.schedule(broker, new RequestList(model, "RequestList", requests), time);
    }

    private void scheduleAcceptResponse(Broker broker, Request response, TimeSpan time) {
        BrokerAcceptResponseEvent responseEvent = new BrokerAcceptResponseEvent(
                model, "BrokerAcceptResponse", true);
        responseEvent.schedule(broker, response, time);
    }
}

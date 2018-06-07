package edu.nyu.tandon.dss.event;

import desmoj.core.simulator.EventOf2Entities;
import desmoj.core.simulator.Model;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.CPU;
import edu.nyu.tandon.dss.entity.Request;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CPURequestProcessedEvent extends EventOf2Entities<CPU, Request> {
    private DistributedSearchSimulation model;

    public CPURequestProcessedEvent(Model model, String s, boolean b) {
        super(model, s, b);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public void eventRoutine(CPU cpu, Request request) {
        Broker broker = model.requestBroker(request.getBroker());
        if (broker != null) { scheduleBrokerAcceptResponse(request);}
        else { request.getBroker().queueQueryResponse(request); }

        Request nextRequest = cpu.getNode().requestAcceptedRequest();
        if (nextRequest != null) { scheduleAcceptRequest(cpu, nextRequest); }
        else { cpu.makeIdle(); }

    }

    private void scheduleBrokerAcceptResponse(Request request) {
        BrokerAcceptResponseEvent event =
                new BrokerAcceptResponseEvent(model, "BrokerAcceptResponse", true);
        event.schedule(request.getBroker(), request);
    }
    
    private void scheduleAcceptRequest(CPU cpu, Request request) {
        CPUAcceptRequestEvent event = new CPUAcceptRequestEvent(
                model, "CPUAcceptRequest", true);
        event.schedule(cpu, request);
    }
}

package edu.nyu.tandon.dss.event;

import desmoj.core.simulator.EventOf2Entities;
import desmoj.core.simulator.Model;
import desmoj.core.simulator.TimeSpan;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.CPU;
import edu.nyu.tandon.dss.entity.Request;

import java.util.concurrent.TimeUnit;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CPUAcceptRequestEvent extends EventOf2Entities<CPU, Request> {
    private DistributedSearchSimulation model;

    public CPUAcceptRequestEvent(Model model, String s, boolean b) {
        super(model, s, b);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public void eventRoutine(CPU cpu, Request request) {
        sendTraceNote(String.format("Processing request %d", request.getQuery().getRequestId()));
        CPURequestProcessedEvent event = new CPURequestProcessedEvent(model, "CPURequestProcessed", true);
        event.schedule(cpu, request, new TimeSpan(model.queryTime(request), TimeUnit.MILLISECONDS));
    }
}

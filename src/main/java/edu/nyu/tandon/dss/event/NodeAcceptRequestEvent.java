package edu.nyu.tandon.dss.event;

import desmoj.core.simulator.EventOf2Entities;
import desmoj.core.simulator.Model;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.CPU;
import edu.nyu.tandon.dss.entity.Node;
import edu.nyu.tandon.dss.entity.Request;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class NodeAcceptRequestEvent extends EventOf2Entities<Node, Request> {
    private DistributedSearchSimulation model;

    public NodeAcceptRequestEvent(Model model, String s, boolean b) {
        super(model, s, b);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public void eventRoutine(Node node, Request request) {
        CPU cpu = node.requestCPU();
        if (cpu !=  null) {
            CPUAcceptRequestEvent event;
            event = new CPUAcceptRequestEvent(model, "CPUAcceptRequest", true);
            event.schedule(cpu, request);
        } else { node.queueAcceptedRequest(request); }

        Request nextRequest = node.requestDispatchedRequest();
        if (nextRequest != null) { schedule(node, nextRequest); }
        else { node.setIdle(); }
    }
}

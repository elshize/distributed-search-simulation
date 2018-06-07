package edu.nyu.tandon.dss.event;

import desmoj.core.simulator.EventOf2Entities;
import desmoj.core.simulator.Model;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Node;
import edu.nyu.tandon.dss.entity.Request;
import edu.nyu.tandon.dss.entity.RequestList;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class BrokerDispatchEvent extends EventOf2Entities<Broker, RequestList> {
    private DistributedSearchSimulation model;

    public BrokerDispatchEvent(Model model, String s, boolean b) {
        super(model, s, b);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public void eventRoutine(Broker broker, RequestList requests) {
        for (Request request : requests.requests) {
            Node node = broker.selectNode(request);
            if (node.isIdle()) {
                node.setIdle(false);
                NodeAcceptRequestEvent event = new NodeAcceptRequestEvent(
                        model, "NodeAcceptRequest", true);
                event.schedule(node, request);
            }
            else {
                node.queueDispatchedRequest(request);
            }
        }
    }
}

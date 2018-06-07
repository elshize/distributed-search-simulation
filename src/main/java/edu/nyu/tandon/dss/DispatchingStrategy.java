package edu.nyu.tandon.dss;

import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Node;
import edu.nyu.tandon.dss.entity.Request;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface DispatchingStrategy {
    Node selectNode(Broker broker, Request request);
}

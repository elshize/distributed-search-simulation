package edu.nyu.tandon.dss.event;

import desmoj.core.dist.DiscreteDistUniform;
import desmoj.core.simulator.Model;
import edu.nyu.tandon.dss.DispatchingStrategy;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Node;
import edu.nyu.tandon.dss.entity.Request;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RandomDispatchingStrategy implements DispatchingStrategy {
    private DiscreteDistUniform uniformDist;
    private DistributedSearchSimulation model;

    public RandomDispatchingStrategy(Model model) {
        uniformDist = new DiscreteDistUniform(model, "DiscreteDistUniform", 0, 1000, true, true);
        this.model = (DistributedSearchSimulation)model;
    }

    @Override
    public Node selectNode(Broker broker, Request request) {
        List<Node> candidates = model.getAllNodes().stream()
                .filter(node -> node.containsShard(request.getShard()))
                .collect(Collectors.toList());
        return candidates.get(uniformDist.sample().intValue() % candidates.size());
    }
}

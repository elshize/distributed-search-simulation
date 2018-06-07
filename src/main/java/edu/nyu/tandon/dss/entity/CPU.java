package edu.nyu.tandon.dss.entity;

import desmoj.core.simulator.Entity;
import desmoj.core.simulator.Model;
import edu.nyu.tandon.dss.DistributedSearchSimulation;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CPU extends Entity {
    protected DistributedSearchSimulation model;
    protected Node node;

    public CPU(Model owner, String name, Node node) {
        super(owner, name, true);
        model = (DistributedSearchSimulation)owner;
        this.node = node;
    }

    public void makeIdle() {
        node.idleCPUs.insert(this);
    }

    public Node getNode() {
        return node;
    }
}

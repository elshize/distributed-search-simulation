package edu.nyu.tandon.dss.entity;

import desmoj.core.simulator.Entity;
import desmoj.core.simulator.Model;

import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RequestList extends Entity {
    public List<Request> requests;
    public RequestList(Model model, String s, List<Request> requests) {
        super(model, s, true);
        this.requests = requests;
    }
}

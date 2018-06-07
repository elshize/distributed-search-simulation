package edu.nyu.tandon.dss.event;

import co.paralleluniverse.fibers.SuspendExecution;
import desmoj.core.simulator.ExternalEvent;
import desmoj.core.simulator.Model;
import desmoj.core.simulator.TimeSpan;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Query;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class QueryGeneratorEvent extends ExternalEvent {
    private DistributedSearchSimulation model;
    private List<String> availableQueries;
    private int nextRequestId = 0;

    public QueryGeneratorEvent(Model owner, String name, List<String> availableQueries) {
        super(owner, name, true);
        model = (DistributedSearchSimulation)owner;
        this.availableQueries = availableQueries;
    }

    @Override
    public void eventRoutine() throws SuspendExecution {
        int id = model.queryIdDist.sample().intValue();
        Query query = new Query(model, availableQueries.get(id), id, nextRequestId++);
        model.newQueries.insert(query);
        Broker broker = model.requestBroker();
        if (broker != null) {
            model.newQueries.remove(query);
            BrokerAcceptQueryEvent event = new BrokerAcceptQueryEvent(model, "BrokerAcceptQuery", true);
            event.schedule(broker, query);
        }
        model.queriesGenerated.update();
        schedule(new TimeSpan(model.queryDist.sample(), TimeUnit.MILLISECONDS));
    }
}

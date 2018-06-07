package edu.nyu.tandon.dss;

import desmoj.core.dist.ContDistExponential;
import desmoj.core.dist.DiscreteDist;
import desmoj.core.dist.DiscreteDistUniform;
import desmoj.core.simulator.Model;
import desmoj.core.simulator.Queue;
import desmoj.core.simulator.TimeSpan;
import desmoj.core.statistic.Accumulate;
import desmoj.core.statistic.Count;
import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Node;
import edu.nyu.tandon.dss.entity.Query;
import edu.nyu.tandon.dss.entity.Request;
import edu.nyu.tandon.dss.event.QueryGeneratorEvent;
import edu.nyu.tandon.dss.event.RandomDispatchingStrategy;
import edu.nyu.tandon.dss.struct.Selection;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class DistributedSearchSimulation extends Model {

    public Queue<Query> newQueries;
    protected Queue<Broker> idleBrokers;
    protected Queue<Node> idleNodes;
    protected List<Node> allNodes;

    protected List<String> queries;
    protected List<List<List<Double>>> queryTimes;
    public DiscreteDist<Long> queryIdDist;

    protected List<Selection> shardRanks;
    protected int budget;

    public ContDistExponential queryDist;

    protected int numBrokers;
    protected int numCPUs;
    protected List<Set<Integer>> nodeConfiguration;
    protected double queryInterval;

    public Count queriesGenerated;
    public Count queriesFinished;

    public Accumulate latencies;

    public DistributedSearchSimulation(Model owner, String modelName, double queryInterval,
                                       List<String> queries, List<List<List<Double>>> queryTimes,
                                       List<Selection> shardRanks, int numBrokers, int numCPUs,
                                       List<Set<Integer>> nodeConfiguration, int budget) {
        super(owner, modelName, true, true);
        this.queryInterval = queryInterval;
        this.queries = queries;
        this.queryTimes = queryTimes;
        this.shardRanks = shardRanks;
        this.numBrokers = numBrokers;
        this.numCPUs = numCPUs;
        this.nodeConfiguration = nodeConfiguration;
        this.budget = budget;
    }

    @Override
    public String description() {
        return "TODO";
    }

    @Override
    public void doInitialSchedules() {
        QueryGeneratorEvent event = new QueryGeneratorEvent(this, "QueryGenerator", queries);
        event.schedule(new TimeSpan(0));
    }

    @Override
    public void init() {
        queryDist = new ContDistExponential(this, "QueryIntervals", queryInterval, true, true);
        queryDist.setNonNegative(true);
        queryIdDist = new DiscreteDistUniform(this, "QueryRandSelection", 0, queries.size() - 1, true, false);
        newQueries = new Queue<Query>(this, "New queries queue", true, false);
        idleBrokers = new Queue<Broker>(this, "Idle brokers queue", true, false);
        idleNodes = new Queue<Node>(this, "Idle nodes queue", true, false);
        allNodes = new ArrayList<>();
        for (int idx = 0; idx < numBrokers; idx++) {
            idleBrokers.insert(new Broker(this, String.format("Broker %d", idx),
                    new RandomDispatchingStrategy(this)));
        }
        int nodeId = 0;
        for (Set<Integer> shardsInNode : nodeConfiguration) {
            Node node = new Node(this, String.format("Node %d", nodeId++), shardsInNode, numCPUs);
            allNodes.add(node);
            idleNodes.insert(node);
        }
        queriesGenerated = new Count(this, "QueriesGenerated", false, false);
        queriesFinished = new Count(this, "QueriesFinished", false, false);
        latencies = new Accumulate(this, "LatencyAccumulator", true, true);
    }

    public List<Node> getAllNodes() {
        return this.allNodes;
    }

    public double queryTime(Request request) {
        double time = 0.0;
        List<Double> bucketTimes = queryTimes.get(request.getQuery().getQueryId())
                .get(request.getShard());
        for (int bucket = 0; bucket < request.getBuckets(); bucket++) {
            time += bucketTimes.get(bucket);
        }
        sendTraceNote(String.format("Shard: %d, Buckets: %d, Time: %f",
                request.getShard(), request.getBuckets(), time));
        return time;
    }

    public List<Request> select(Broker broker, Query query) {
        return shardRanks.get(query.getQueryId()).select(broker, query, budget);
    }

    /**
     * Requests an idle broker.
     * If any idle brokers wait, it removes it from the queue and returns it.
     * Otherwise, null is returned.
     * @return broker or null if no idle brokers
     */
    public Broker requestBroker() {
        if (idleBrokers.isEmpty()) return null;
        Broker broker = idleBrokers.first();
        idleBrokers.remove(broker);
        return broker;
    }

    /**
     * Requests the given broker.
     * If it is idle, it is returned. Otherwise, null is returned.
     * @return broker or null if the broker is idle
     */
    public Broker requestBroker(Broker broker) {
        if (!idleBrokers.contains(broker)) return null;
        idleBrokers.remove(broker);
        return broker;
    }

    /**
     * Queues a broker for further use; the broker becomes idle.
     * @param broker
     */
    public void queueBroker(Broker broker) {
        idleBrokers.insert(broker);
    }

    public Query requestQuery() {
        if (newQueries.isEmpty()) return null;
        Query query = newQueries.first();
        newQueries.remove(query);
        return query;
    }
}

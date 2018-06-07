package edu.nyu.tandon.dss.struct;

import edu.nyu.tandon.dss.entity.Broker;
import edu.nyu.tandon.dss.entity.Query;
import edu.nyu.tandon.dss.entity.Request;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Selection {
    private List<Pick> picks;

    public Selection(List<Pick> picks) {
        this.picks = picks;
    }

    public List<Request> select(Broker broker, Query query, int budget) {
        Map<Integer, List<Pick>> m = picks.stream().limit(budget).sorted()
                .collect(Collectors.groupingBy(Pick::getShard, Collectors.toList()));
        int selectedShards = m.size();
        return m.entrySet().stream()
                .map(e -> new Request(broker, query, e.getKey(),
                        e.getValue().stream().collect(Collectors.maxBy(Pick::compareTo)).get().getBucket() + 1,
                        selectedShards))
                .collect(Collectors.toList());
    }
}

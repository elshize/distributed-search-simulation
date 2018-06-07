import com.beust.jcommander.Parameter;
import desmoj.core.simulator.Experiment;
import desmoj.core.simulator.TimeInstant;
import edu.nyu.tandon.dss.DistributedSearchSimulation;
import edu.nyu.tandon.dss.config.RandomConfigGenerator;
import edu.nyu.tandon.dss.struct.Pick;
import edu.nyu.tandon.dss.struct.Selection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * This Java source file was generated by the Gradle 'init' task.
 */
public class App {

    @Parameter(names = {"--config", "-c"}, required = true)
    String configPath = null;

    private static List<String> loadQueries(String filename) throws IOException {
        return Files.readAllLines(Paths.get(filename));
    }

    private static List<Double> string2Doubles(String line) {
        return Arrays.asList(line.split("\\s+")).stream()
                .map(Double::parseDouble).collect(Collectors.toList());
    }

    private static List<Integer> line2Ints(String line) {
        return Arrays.asList(line.split("\\s+")).stream()
                .map(Integer::parseInt).collect(Collectors.toList());
    }

    private static Pick parsePick(String str) {
        String[] split = str.split(":");
        return new Pick(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
    }

    private static Selection line2Selection(String line) {
        return new Selection(Arrays.asList(line.split("\\s+")).stream()
                .map(App::parsePick).collect(Collectors.toList()));
    }

    private static List<List<Double>> line2Costs(String line) {
        return Arrays.stream(line.trim().split("\t"))
                .map(App::string2Doubles).collect(Collectors.toList());
    }

    private static List<List<List<Double>>> loadQueryTimes(String filename) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            return stream.map(App::line2Costs).collect(Collectors.toList());
        }
    }

    private static List<Selection> loadShardRanks(String filename) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            return stream.map(App::line2Selection).collect(Collectors.toList());
        }
    }

    private static List<Set<Integer>> loadNodeConfiguration(String filename) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            return stream.map(App::line2Ints).map(HashSet<Integer>::new)
                    .collect(Collectors.toList());
        }
    }

    public static void main(String[] args) throws IOException {
        //List<String> queries = Arrays.asList("query 1", "query 2", "query 3");
        //List<List<Double>> queryTimes = Arrays.asList(
        //        Arrays.asList(1.0, 1.0, 1.0),  // query 1
        //        Arrays.asList(1.0, 1.0, 1.0),  // query 2
        //        Arrays.asList(1.0, 1.0, 1.0)   // query 3
        //);
        //List<Set<Integer>> nodeConfiguration = Arrays.asList(
        //        //new HashSet<Integer>(Arrays.asList(0, 1, 2))
        //        new HashSet<Integer>(Arrays.asList(0, 1)),
        //        new HashSet<Integer>(Arrays.asList(1, 2)),
        //        new HashSet<Integer>(Arrays.asList(0, 2))
        //);
        List<String> queries = loadQueries("/home/elshize/phd/oss/data/clueweb/cw09b-trec_eval-queries.txt");
        List<Selection> shardRanks = loadShardRanks("/home/elshize/phd/oss/data/clueweb/selection.b20.sim");
        List<Set<Integer>> nodeConfiguration = loadNodeConfiguration("/home/elshize/IdeaProjects/distributed-search-simulation/cw-16n-rand.config");
        //List<Set<Integer>> nodeConfiguration = new RandomConfigGenerator(10, 123, 4, new Random(0)).generate();
        List<List<List<Double>>> times = loadQueryTimes("/home/elshize/phd/oss/data/clueweb/mscost-20.sim");
        double queryInterval = 10;
        int numBrokers = 1;
        int numCPUs = 16;
        int budget = 100;
        DistributedSearchSimulation model = new DistributedSearchSimulation(null,
                "DSS", queryInterval, queries, times, shardRanks,
                numBrokers, numCPUs, nodeConfiguration, budget);
        Experiment exp = new Experiment("DSSExperiment");
        model.connectToExperiment(exp);
        TimeInstant experimentTime = new TimeInstant(20, TimeUnit.SECONDS);
        exp.setShowProgressBar(false);
        exp.stop(experimentTime);
        exp.tracePeriod(new TimeInstant(0), new TimeInstant(2, TimeUnit.SECONDS));
        exp.debugPeriod(new TimeInstant(0), new TimeInstant(2, TimeUnit.SECONDS));
        exp.start();
        exp.report();
        exp.finish();
        double throughput = (double)model.queriesFinished.getValue()
                / experimentTime.getTimeAsDouble(TimeUnit.SECONDS);
        System.out.println(String.format("Time: %f sec", experimentTime.getTimeAsDouble(TimeUnit.SECONDS)));
        System.out.println(String.format("Queries generated: %d", model.queriesGenerated.getValue()));
        System.out.println(String.format("Queries finished: %d", model.queriesFinished.getValue()));
        System.out.println(String.format("Throughput: %f queries/sec", throughput));
        System.out.println(String.format("Average latency: %f ms", model.latencies.getMean()));
    }
}

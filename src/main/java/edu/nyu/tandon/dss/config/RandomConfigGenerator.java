package edu.nyu.tandon.dss.config;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RandomConfigGenerator {
    @Parameter(names = {"--nodes", "-n"}, required = true)
    int nodes;

    @Parameter(names = {"--shards", "-s"}, required = true)
    int shards;

    @Parameter(names = {"--duplication", "-d"}, required = true)
    int duplicationFactor;

    Random random;

    public RandomConfigGenerator() {
        this.random = new Random();
    }

    public RandomConfigGenerator(int nodes, int shards, int duplicationFactor, Random random) {
        assert(nodes >= duplicationFactor);
        this.nodes = nodes;
        this.shards = shards;
        this.duplicationFactor = duplicationFactor;
        this.random = random;
    }

    public List<Set<Integer>> generate() {
        List<Set<Integer>> config = new ArrayList<>();
        for (int node = 0; node < nodes; node++) {
            config.add(new HashSet<>());
        }
        for (int shard = 0; shard < shards; shard++) {
            List<Integer> nodeList = Stream.iterate(0, i -> i + 1)
                    .limit(nodes)
                    .collect(Collectors.toList());
            Collections.shuffle(nodeList);
            final int shardId = shard;
            nodeList.stream().limit(duplicationFactor)
                    .forEach(node -> config.get(node).add(shardId));
        }
        return config;
    }

    public static void main(String[] args) {
        RandomConfigGenerator rcg = new RandomConfigGenerator();
        try {
            JCommander.newBuilder()
                    .addObject(rcg)
                    .build()
                    .parse(args);
            List<Set<Integer>> config = rcg.generate();
            for (Set<Integer> shards : config) {
                System.out.println(String.join(" ",
                        shards.stream().map(String::valueOf).collect(Collectors.toList())));
            }
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
        }
    }
}

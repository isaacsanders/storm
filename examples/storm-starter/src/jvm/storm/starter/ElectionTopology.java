package storm.starter;

import backtype.storm.Config;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.*;
import storm.starter.util.StormRunner;

/**
 * Created by isaac on 10/31/15.
 */
public class ElectionTopology {
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
    private static final int TOP_N = 5;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public ElectionTopology(String topologyName) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        String booth1Id = "booth1";
        String booth2Id = "booth2";
        String booth3Id = "booth3";
        String booth4Id = "booth4";
        String booth5Id = "booth5";

        String votingCenter1Id = "center1";
        String votingCenter2Id = "center2";
        String votingCenter3Id = "center3";

        String ohio = "ohio";
        String indiana = "indiana";

        builder.setSpout(booth1Id, new TestWordSpout(), 1);
        builder.setSpout(booth2Id, new TestWordSpout(), 2);
        builder.setSpout(booth3Id, new TestWordSpout(), 3);
        builder.setSpout(booth4Id, new TestWordSpout(), 4);
        builder.setSpout(booth5Id, new TestWordSpout(), 5);

        builder.setBolt(votingCenter1Id, new VotingCenterBolt("Vigo"), 4).fieldsGrouping(booth1Id, new Fields("word"));

        BoltDeclarer franklinCounty = builder.setBolt(votingCenter2Id, new VotingCenterBolt("Franklin"), 4);
        franklinCounty.shuffleGrouping(booth2Id);
        franklinCounty.shuffleGrouping(booth5Id);

        BoltDeclarer hamiltonCounty = builder.setBolt(votingCenter3Id, new VotingCenterBolt("Hamilton"), 4);
        hamiltonCounty.shuffleGrouping(booth3Id);
        hamiltonCounty.shuffleGrouping(booth4Id);

        builder.setBolt(ohio, new ElectoralCollegeBolt("OH"), 4).shuffleGrouping(votingCenter2Id);
        BoltDeclarer inec = builder.setBolt(indiana, new ElectoralCollegeBolt("IN"));
        inec.shuffleGrouping(votingCenter1Id);
        inec.shuffleGrouping(votingCenter3Id);

        BoltDeclarer print = builder.setBolt("printer", new PrinterBolt());
        print.shuffleGrouping(ohio);
        print.shuffleGrouping(indiana);
    }

    public void runLocally() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public void runRemotely() throws Exception {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    }

    public static void main(String[] args) throws Exception {
        String topologyName = "electionTopology";
        if (args.length >= 1) {
            topologyName = args[0];
        }
        boolean runLocally = true;
        if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
            runLocally = false;
        }

        ElectionTopology election = new ElectionTopology(topologyName);
        if (runLocally) {
            election.runLocally();
        }
        else {
            election.runRemotely();
        }
    }
}

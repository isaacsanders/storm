package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by isaac on 10/31/15.
 */
public class ElectoralCollegeBolt extends BaseRichBolt {
    private final String _state;
    private Map<String, List<String>> _totals;
    private Integer _leaderVotes;
    private OutputCollector _collector;

    public ElectoralCollegeBolt(String state) {
        _state = state;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _totals = new HashMap<>();
        _leaderVotes = 0;
    }

    @Override
    public void execute(Tuple input) {
        List<String> incomingVotes = (List<String>) input.getValueByField("votes");

        String newLeader = null;
        Integer leaderVotes = _leaderVotes;

        for (String vote : incomingVotes) {
            List votesForCandidate = _totals.get(vote);
            if (votesForCandidate == null) {
                votesForCandidate = new ArrayList();
                _totals.put(vote, votesForCandidate);
            } else {
                votesForCandidate.add(vote);
            }


            if (votesForCandidate.size() > leaderVotes) {
                newLeader = vote;
                leaderVotes = votesForCandidate.size();
            }
        }

        if (newLeader != null && _leaderVotes != leaderVotes) {
            _collector.emit(new Values(_state, newLeader));
            _leaderVotes = leaderVotes;
        }

        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("state", "newLeader")); }
}

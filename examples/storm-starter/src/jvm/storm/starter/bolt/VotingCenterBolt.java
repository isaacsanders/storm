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
public class VotingCenterBolt extends BaseRichBolt {
    private final String _county;
    private OutputCollector _collector;
    private List<String> _votes;
    private Integer _limit;

    public VotingCenterBolt(String county) {
        _county = county;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _votes = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        String vote = input.getStringByField("word");
        _votes.add(vote);
        if (_votes.size() >= 10) {
            _collector.emit(new Values(_county, _votes));
            _votes = new ArrayList<>();
        }
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("county", "votes"));
    }
}

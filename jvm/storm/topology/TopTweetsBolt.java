package storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import storm.topology.tools.*;

/**
 * Main driver for processing top tweets.
 * Records top hash-tags from the total-ranker bolt
 * and emits new tweets containing top hash-tags.
 */

public class TopTweetsBolt extends BaseRichBolt
{

  private OutputCollector collector;
  private Map<String, Long> hashTags;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
    hashTags = new HashMap<String, Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    String component = tuple.getSourceComponent();
    if (component.equals("total-ranker")) {
      Rankings rankableList = (Rankings) tuple.getValue(0);
      for (Rankable r : rankableList.getRankings()){
        String hashTag = r.getObject().toString();
        Long count = r.getCount();
        hashTags.put(hashTag, count);
      }
    }
    else if (component.equals("tweet-spout")) {
      String tweet = tuple.getString(0);
      String[] tokens = tweet.split("[ .,?!]+");
      for (String token : tokens) {
        if (hashTags.containsKey(token)) {
          collector.emit(new Values(tweet, hashTags.get(token)));
          break;
        }
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("tweet","count"));
  }
}

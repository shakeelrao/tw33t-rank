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

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import storm.topology.tools.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A bolt which publishes the word/count of a hash-tag to redis; included as an example.
 */

public class ReportBolt extends BaseRichBolt {

  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    RedisClient client = new RedisClient("localhost", 6379);
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple) {
    String tweet = tuple.getString(0);
    Long count = (Long) tuple.getValue(1);
    redis.publish("WordCountTopology", tweet + "|" + count);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}

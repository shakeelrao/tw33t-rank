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

class TweetRankTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    int RANKING = 10;

    // enter Twitter API keys (https://apps.twitter.com/)
    TweetSpout tweetSpout = new TweetSpout("custKey", "custSecret", "accessToken", "acessSecret");

    builder.setSpout("tweet-spout", tweetSpout, 1);
    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");
    builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
    builder.setBolt("intermediate-ranker", new IntermediateRankingsBolt(RANKING), 4).fieldsGrouping("count-bolt", new Fields("word"));
    builder.setBolt("total-ranker", new TotalRankingsBolt(RANKING)).globalGrouping("intermediate-ranker");
    builder.setBolt("top-tweets", new TopTweetsBolt(), 1).globalGrouping("total-ranker").globalGrouping("tweet-spout");
    // attach top-tweet to a vizualization bolt
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("top-tweets");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    else {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());
      Utils.sleep(300000);
      cluster.killTopology("tweet-word-count");
      cluster.shutdown();
    }
  }
}

//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;


/**
 * Created by Felix on 16/10/24.
 */
public class WordCountShellBoltTopology {
  public static class SplitSentence extends ShellBolt implements IRichBolt {
    private static final long serialVersionUID = -6200054063827257524L;

    public SplitSentence() {
      super("python", "splitsentence.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
    private static final long serialVersionUID = 2606916532020258152L;
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  // Utils class to generate random String at given length
  public static class RandomString {
    private final char[] symbols;

    private final Random random = new Random();

    private final char[] buf;

    public RandomString(int length) {
      // Construct the symbol set
      StringBuilder tmp = new StringBuilder();
      for (char ch = '0'; ch <= '9'; ++ch) {
        tmp.append(ch);
      }

      for (char ch = 'a'; ch <= 'z'; ++ch) {
        tmp.append(ch);
      }

      symbols = tmp.toString().toCharArray();
      if (length < 1) {
        throw new IllegalArgumentException("length < 1: " + length);
      }

      buf = new char[length];
    }

    public String nextString() {
      for (int idx = 0; idx < buf.length; ++idx) {
        buf[idx] = symbols[random.nextInt(symbols.length)];
      }

      return new String(buf);
    }
  }

  /**
   * A spout that emits a random word
   */
  public static class WordSpout extends BaseRichSpout {
    private static final long serialVersionUID = 4322775001819135036L;

    private static final int ARRAY_LENGTH = 128 * 1024;
    private static final int WORD_LENGTH = 20;

    private final String[] words = new String[ARRAY_LENGTH];

    private final Random rnd = new Random(31);

    private SpoutOutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      RandomString randomString = new RandomString(WORD_LENGTH);
      for (int i = 0; i < ARRAY_LENGTH; i++) {
        words[i] = randomString.nextString();
      }

      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      int nextInt = rnd.nextInt(ARRAY_LENGTH);
      collector.emit(new backtype.storm.tuple.Values(words[nextInt]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  /**
   * Main method
   */
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    if (args.length < 1) {
      throw new RuntimeException("Please Specify topology name");
    }

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new WordSpout(), parallelism);
    builder.setBolt("split", new SplitSentence()).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount()).fieldsGrouping("split", new Fields("word"));
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);

    conf.put(Config.TOPOLOGY_MULTILANG_SERIALIZER,"org.apache.storm.multilang.JsonSerializer");
    conf.put(Config.TOPOLOGY_SHELLBOLT_MAX_PENDING,100);
    conf.put(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS,2);
    conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS,2);


    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}

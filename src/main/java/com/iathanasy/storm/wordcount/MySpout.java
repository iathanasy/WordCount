package com.iathanasy.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.Random;

public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    Random random;
    String [] sentences=null;

    //初始化
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        random = new Random();
        sentences = new String[]{"How to improve your email writing skills ?","i am lilei love hanmeimei","all the years round","as a matter of fact","as best one can"};
    }

    //storm 框架在 while(true) 调用nextTuple方法
    @Override
    public void nextTuple() {
        //Utils.sleep(10000);
        //获取数据
        String sentence  = sentences[random.nextInt(sentences.length)];
        System.out.println("线程名："+Thread.currentThread().getName()+"  "+new DateTime().toString("yyyy-MM-dd HH:mm:ss  ")+"10s发射一次数据："+sentence);
        //像下游发射数据
        this.collector.emit(new Values(sentence));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}

package com.iathanasy.storm.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * 简单的按照空格进行拆分，发射到下一阶段的bolt
 */
public class SplitWordBolt extends BaseRichBolt{

    OutputCollector collector;
    /**
     * 初始化
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    //被storm框架循环调用，传入参数tuple
    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] split = sentence.split(" ");
        for(String word : split){
            //根据空格拆分 发射到下一个 bolt
            collector.emit(new Values(word));//发送split
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明输出的filed
        declarer.declare(new Fields("word"));
    }
}

package com.iathanasy.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 计数统计
 */
public class WordCount {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //1、准备一个TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();
        //设置数据源
        builder.setSpout("spout",new MySpout(),1);
        //读取spout数据源的数据，进行split业务逻辑
        builder.setBolt("split",new SplitWordBolt(),2).shuffleGrouping("spout");
        //读取split后的数据， 进行count (tick周期10s)
        builder.setBolt("count",new SumWordBlot(),4).fieldsGrouping("split",new Fields("word"));

        //2、创建一个configuration，用来指定当前topology 需要的worker的数量
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);

        //3丶提交任务  集群模式  和本地模式

        //3.1  集群模式
       // StormSubmitter.submitTopology("wordcount",config,builder.createTopology());

        //3.2 本地模式
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount",config,builder.createTopology());
    }
}

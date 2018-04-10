package com.iathanasy.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class SumWordBlot extends BaseRichBolt{
    OutputCollector collector;
    Map<String,Integer> counts=new HashMap<>();

    //初始化
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

   /* @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String,Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,10);//加人tick时间窗口，进行统计
        return conf;
    }*/

    @Override
    public void execute(Tuple tuple) {
       /* //时间窗口定义为10s内的统计数据，统计完毕后，发射到下一阶段的bolt进行处理
        //发射完成后retun结束，开始新一轮的时间窗口计数操作
        if(TupleHelpers.isTickTuple(tuple)){
            System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss")+" 每隔10s发射一次map 大小："+counts.size());
//            Map<String,Integer> copyMap= (Map<String, Integer>) deepCopy(counts);
            outputCollector.emit(new Values(counts));//10S发射一次
//            counts.clear();
            counts=new HashMap<>();//这个地方，不能执行clear方法，可以再new一个对象，否则下游接受的数据，有可能为空 或者深度copy也行，推荐new
            return;
        }*/

        //如果没到发射时间，就继续统计wordcount
        //System.out.println("线程"+Thread.currentThread().getName()+"  map 缓冲统计中......  map size："+counts.size());
        //String word=tuple.getString(0);//如果有多tick，就不用使用这种方式获取tuple里面的数据
        String word = tuple.getStringByField("word");
        Integer count = counts.get(word);
        if(count == null){
            count = 0;
        }
        count++;
        counts.put(word,count);
         System.out.println(word+" =====>  "+count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       // declarer.declare(new Fields("word_map"));
    }
}

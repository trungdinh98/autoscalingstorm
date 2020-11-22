package com.storm.iotdata.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.storm.iotdata.models.*;

public class Bolt_forecast extends BaseRichBolt {
    private StormConfig config;
    private int window;

    public Bolt_forecast(Integer window, StormConfig config){
        this.window = window;
        this.config = config;
    }


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub

    }

    @Override
    public void execute(Tuple input) {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }
    
}

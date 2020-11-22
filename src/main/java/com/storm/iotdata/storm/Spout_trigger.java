package com.storm.iotdata.storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Spout_trigger extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private long start = System.currentTimeMillis();
    public Integer interval = 1;

    public Spout_trigger(Integer interval) {
        this.interval = interval;
        this.start = System.currentTimeMillis();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            System.out.println("[Spout-trigger] Sleeping for "+interval+" second(s)");
            Thread.sleep(interval * 1000);
            File tempFolder = new File("./tmp");
            File[] spoutLogs = tempFolder.listFiles();
            Long speed = Long.valueOf(0), load = Long.valueOf(0), total = Long.valueOf(0);
            for (File Log : spoutLogs) {
                try {
                    if(Log.getName().contains("spout_data_log_")){
                        BufferedReader logReader = new BufferedReader(new FileReader(Log));
                        String[] datas = logReader.readLine().split("|");
                        if(datas.length >= 3){
                            speed+=Long.valueOf(datas[0]);
                            load+=Long.valueOf(datas[1]);
                            total+=Long.valueOf(datas[2]);
                        }
                        logReader.close();
                    }
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            _collector.emit(new Values(start, speed, load, total), System.currentTimeMillis()); // Trigger signal to write data to file after 1 min
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trigger", "spoutSpeed", "spoutLoad", "spoutTotal"));
    }
}
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata.storm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

public class Spout_data implements MqttCallback, IRichSpout {

    private SpoutOutputCollector _collector;
    ConcurrentLinkedQueue<String> messages;
    Long total = Long.valueOf(0);
    Long success = Long.valueOf(0);
    Long fail = Long.valueOf(0);
    Long speed = Long.valueOf(0);
    Long load = Long.valueOf(0);
    Long last = System.currentTimeMillis();
    Integer count = 0;
    String brokerUrl = "localhost";
    String clientId = "";
    MqttClient client;
    String topic = "#";

    public Spout_data(String broker_url, String topic) {
        this.brokerUrl = broker_url;
        this.topic = topic;
        messages = new ConcurrentLinkedQueue<String>();
        if (!(new File("tmp").isDirectory())) {
            new File("tmp").mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(new File("tmp"));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        messages.add(message.toString());
        total++;
        speed++;
    }

    public void connectionLost(Throwable cause) {
        System.out.println("[Spout-data-" + topic + "] Lost connection with broker. Trying to reconnect..");
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        System.out.println("[Spout-data-" + topic + "] Connecting to broker (" + brokerUrl + ")..");
        try {
            client = new MqttClient(brokerUrl, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setConnectionTimeout(10);
            client.connect(options);
            System.out.println("[Spout-data-" + topic + "] Connected to broker (" + brokerUrl + ").");
            client.setCallback(this);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        new File("/tmp/spout_log_"+ topic +".tmp").delete();
    }

    public void activate() {
        if (client.isConnected()) {
            try {
                client.subscribe(topic);
                System.out.println("[Spout-data-" + topic + "] Subscribed to topic " + topic + ".");
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            try {
                client.connect();
                client.subscribe(topic);
            } catch (MqttSecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
	}

	public void deactivate() {
        if(client.isConnected()){
            try {
                client.unsubscribe(topic);
                System.out.println("[Spout-data-" + topic + "] Unsubscribed topic " + topic + ".");
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else{
            try {
                client.reconnect();
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
	}

	public void nextTuple() {
		if(messages.isEmpty()){
            return;
        }
        String message = messages.poll();
        String[] metric = message.split(",");
        if (Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
            _collector.emit(new Values(metric[1], metric[2], metric[3], metric[4], metric[5], metric[6]), message);
            load++;
        }
        if(++count>10000){
            count=0;
            try {
                FileWriter log = new FileWriter(new File("tmp/spout_data_log_"+ topic +".tmp"), false);
                PrintWriter pwOb = new PrintWriter(log , false);
                pwOb.flush();
                log.write(speed*1000/(System.currentTimeMillis()-last)+"|"+load*1000/(System.currentTimeMillis()-last)+"|"+total+"|"+messages.size()+"|"+success+"|"+fail);
                pwOb.close();
                log.close();
                speed = Long.valueOf(0);
                load = Long.valueOf(0);
                last=System.currentTimeMillis();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
	}

	public void ack(Object msgId) {
        success++;
    }

	public void fail(Object msgId) {
        fail++;
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* uses default stream id */
        declarer.declare(new Fields("timestamp", "value", "property", "plugId","householdId", "houseId"));
    }    
}

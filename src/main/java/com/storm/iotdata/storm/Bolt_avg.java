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
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.storm.iotdata.models.*;
import com.storm.iotdata.functions.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author hiiamlala
 */
public class Bolt_avg extends BaseRichBolt {
    private StormConfig config;
    public Integer gap;
    public Integer triggerCount = 0;
    public Double total = Double.valueOf(0);
    public HashMap<String, DeviceData> deviceDataList = new HashMap<String, DeviceData>();
    public HashMap<String, DeviceProp> devicePropList = new HashMap<String, DeviceProp>();

    public Bolt_avg(Integer gap, StormConfig config) {
        this.gap = gap;
        this.config = config;
        devicePropList = DB_store.initDevicePropList();
    }
    
    private OutputCollector _collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            if(tuple.contains("trigger")){
                if(((++triggerCount)%gap)==0){
                    triggerCount = 0;
                    Long startTime = (Long) tuple.getValueByField("trigger");
                    Long spoutSpeed = (Long) tuple.getValueByField("spoutSpeed");
                    Long spoutLoad = (Long) tuple.getValueByField("spoutLoad");
                    Long spoutTotal = (Long) tuple.getValueByField("spoutTotal");

                    Long startExec = System.currentTimeMillis();
                    Stack<String> needClean = new Stack<String>();
                    Stack<DeviceData> needSave = new Stack<DeviceData>();
                    Stack<DeviceNotification> deviceNotificationList = new Stack<DeviceNotification>();

                    //Init data for save and clean procedure
                    for(String key : deviceDataList.keySet()){
                        DeviceData data = deviceDataList.get(key);
                        if(!data.isSaved()){
                            _collector.emit(new Values(data.getHouseId(), data.getHouseholdId(), data.getDeviceId(), data.getYear(), data.getMonth(), data.getDay(), data.getIndex(), data.getAvg()));
                            needSave.push(data);
                        }
                        else if(data.isSaved() && (System.currentTimeMillis()-data.getLastUpdate())>(60000*gap)){
                            needClean.push(key);
                        }
                    }

                    //DB store data
                    if(DB_store.pushDeviceData(needSave, new File("./tmp/deviceData2db-" + gap + ".lck"))){
                        for(DeviceData deviceData : needSave){
                            deviceDataList.get(deviceData.getUniqueId()).save();
                        }
                    }

                    //DB store prop
                    Stack<DeviceProp> tempDevicePropList = new Stack<DeviceProp>();
                    tempDevicePropList.addAll(devicePropList.values());
                    if(DB_store.pushDeviceProp(tempDevicePropList, new File("./tmp/deviceProp2db-"+ gap + ".lck"))){
                        for(DeviceProp deviceProp : tempDevicePropList){
                            devicePropList.get(deviceProp.getDeviceUniqueId()).save();
                        }
                    }

                    //Check outlier
                    for(DeviceData deviceData : needSave){
                        String devicePropUniqueId = deviceData.getDeviceUniqueId();
                        DeviceProp deviceProp = devicePropList.getOrDefault(devicePropUniqueId, new DeviceProp(deviceData.houseId, deviceData.householdId, deviceData.deviceId, this.gap));
                        if(config.isDeviceCheckMax() && deviceProp.getMax()!=0 && (deviceData.getAvg()-deviceProp.getMax())>=(deviceProp.getMax()*config.getDeviceLogGap()/100)){
                            //Check over Max
                            deviceNotificationList.push(new DeviceNotification(1, deviceData, deviceProp));
                        }
                        if(config.isDeviceCheckAvg() && deviceProp.getAvg()!=0 && (deviceData.getAvg()-deviceProp.getAvg())>=(deviceProp.getAvg()*config.getDeviceLogGap()/100)){
                            //Check over Avg
                            deviceNotificationList.push(new DeviceNotification(0, deviceData, deviceProp));
                        }
                        if(config.isDeviceCheckMin() && deviceProp.getMin()!=0 && (deviceProp.getMin()-deviceData.getAvg())<=(deviceProp.getMin()*config.getDeviceLogGap()/100)){
                            //Check under Min
                            deviceNotificationList.push(new DeviceNotification(-1, deviceData, deviceProp));
                        }
                        //Save data_prop
                        devicePropList.put(devicePropUniqueId, deviceProp.addValue(deviceData.getAvg()));
                    }

                    //Publish Noti
                    if(config.isNotificationMQTT()){
                        MQTT_publisher.deviceNotificationsPublish(deviceNotificationList, config.getSpoutBrokerURL(), config.getMqttTopicPrefix());
                    }
                    
                    //Save Noti
                    if(DB_store.pushDeviceNotification(deviceNotificationList, new File("./tmp/devicenoti2db-" + gap + ".lck"))){
                        //Noti saved
                        // System.out.printf("\n[Bolt_avg_%-3d] Saved to DB %-10d notifications\n", gap, noti_list.size());
                    }
                    
                    //Logging
                    Long execTime = System.currentTimeMillis() - startExec;
                    System.out.printf("\n[Bolt_avg_%-3d] Noti list: %-10d\n", gap, deviceNotificationList.size());
                    System.out.printf("\n[Bolt_avg_%-3d] Total: %-10d | Already saved: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, deviceDataList.size(), deviceDataList.size()-needSave.size(), needSave.size(), needClean.size());
                    System.out.printf("\n[Bolt_avg_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000);

                    Stack<String> stormLogList = new Stack<String>();
                    stormLogList.push(String.format("[Bolt_avg_%-3d] Noti list: %-10d\n", gap, deviceNotificationList.size()));
                    stormLogList.push(String.format("[Bolt_avg_%-3d] Total: %-10d | Already saved: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, deviceDataList.size(), deviceDataList.size()-needSave.size(), needSave.size(), needClean.size()));
                    stormLogList.push(String.format("[Bolt_avg_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                    MQTT_publisher.stormLogPublish(stormLogList, config.getNotificationBrokerURL(), config.getMqttTopicPrefix());

                    try {
                        FileWriter log = new FileWriter(new File("tmp/bolt_avg_"+ gap +".tmp"), false);
                        PrintWriter pwOb = new PrintWriter(log , false);
                        pwOb.flush();
                        log.write(String.format("[Bolt_avg_%-3d] Noti list: %-10d\n", gap, deviceNotificationList.size()));
                        log.write(String.format("[Bolt_avg_%-3d] Total: %-10d | Already saved: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, deviceDataList.size(), deviceDataList.size()-needSave.size(), needSave.size(), needClean.size()));
                        log.write(String.format("[Bolt_avg_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                        pwOb.close();
                        log.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                    //Clean garbage
                    for(String key : needClean){
                        deviceDataList.remove(key);
                    }

                }
            }
            else{
                Integer houseId        = (Integer) tuple.getValueByField("houseId");
                Integer householdId    = (Integer)tuple.getValueByField("householdId");
                Integer deviceId       = (Integer)tuple.getValueByField("deviceId");
                String year             = (String)tuple.getValueByField("year");
                String month            = (String)tuple.getValueByField("month");
                String day              = (String)tuple.getValueByField("day");
                Integer index       = (Integer) tuple.getValueByField("index");
                Double  value           = (Double) tuple.getValueByField("value");
                DeviceData deviceData = new DeviceData(houseId, householdId, deviceId, year, month, day, index, gap);
                deviceDataList.put(deviceData.getUniqueId(), deviceDataList.getOrDefault(deviceData.getUniqueId(), deviceData ).increaseValue(value));
            }
            _collector.ack(tuple);
        }catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("houseId","householdId","deviceId","year","month","day","index","avg"));
    }
    
}
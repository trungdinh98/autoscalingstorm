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
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.storm.iotdata.functions.*;
import com.storm.iotdata.models.*;

/**
 *
 * @author hiiamlala
 */
public class Bolt_sum extends BaseRichBolt {
    private StormConfig config;
    public Integer gap;
    public Integer triggerCount = 0;
    private OutputCollector _collector;
    public HashMap<String, HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > > allData = new HashMap<String, HashMap<Integer,HashMap<Integer,HashMap<String, DeviceData> > > >();
    public HashMap <Integer, HashMap<String, HouseData> > finalHouseDataList = new HashMap <Integer, HashMap<String, HouseData> >();
    public HashMap <String, HashMap<String, HouseholdData> > finalHouseholdDataList = new HashMap <String, HashMap<String, HouseholdData> >();
    public HashMap <String, HouseProp> housePropList = new HashMap<String, HouseProp>();
    public HashMap <String, HouseholdProp> householdPropList = new HashMap<String, HouseholdProp>();

    
    public Bolt_sum(int gap, StormConfig config) {
        this.gap = gap;
        this.config = config;
        this.housePropList = DB_store.initHousePropList();
        this.householdPropList = DB_store.initHouseholdPropList();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("houseId", "year", "month", "day", "index", "value"));
    }

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
                    Integer allDataSize = 0;
                    Integer finalHouseDataSize = 0;
                    Integer finalHouseholdDataSize = 0;
                    Stack<HouseData> houseDataNeedSave = new Stack<HouseData>();
                    Stack<HouseholdData> householdDataNeedSave = new Stack<HouseholdData>();
                    Stack<HouseData> houseDataNeedClean = new Stack<HouseData>();
                    Stack<HouseholdData> householdDataNeedClean = new Stack<HouseholdData>();
                    Stack<String> timesliceNeedClean = new Stack<String>();
                    Stack<HouseNotification> houseNotificationList = new Stack<HouseNotification>();
                    Stack<HouseholdNotification> householdNotificationList = new Stack<HouseholdNotification>();

                    // Cal sum
                    for(String timeslice : allData.keySet()){
                        HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > sliceData = allData.get(timeslice);
                        for(Integer houseId : sliceData.keySet()) {
                            Double houseValue = Double.valueOf(0);
                            HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.get(houseId);
                            for(Integer householdId : houseData.keySet()) {
                                Double householdValue = Double.valueOf(0);
                                HashMap<String, DeviceData> householdData = houseData.get(householdId);
                                for(String dataId : householdData.keySet()) {
                                    allDataSize++;
                                    DeviceData data = householdData.get(dataId);
                                    houseValue+=data.getAvg();
                                    householdValue+=data.getAvg();
                                }
                                HouseholdData calHouseholdData = new HouseholdData(houseId, householdId, timeslice, householdValue);
                                HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdDataList.getOrDefault(calHouseholdData.getHouseholdUniqueId(), new HashMap<String, HouseholdData>());
                                HouseholdData tempHouseholdData = tempFinalHouseholdData.getOrDefault(calHouseholdData.getSliceId(), calHouseholdData);
                                tempHouseholdData.setValue(householdValue);
                                tempFinalHouseholdData.put(calHouseholdData.getSliceId(), tempHouseholdData);
                                finalHouseholdDataList.put(calHouseholdData.getHouseholdUniqueId(), tempFinalHouseholdData);
                            }
                            HouseData calHouseData = new HouseData(houseId, timeslice, houseValue);
                            HashMap<String, HouseData> tempFinalHouseData = finalHouseDataList.getOrDefault(calHouseData.getHouseId(), new HashMap<String, HouseData>());
                            HouseData tempHouseData = tempFinalHouseData.getOrDefault(calHouseData.getSliceId(), calHouseData);
                            tempHouseData.setValue(houseValue);
                            tempFinalHouseData.put(calHouseData.getSliceId(), tempHouseData);
                            finalHouseDataList.put(calHouseData.getHouseId(), tempFinalHouseData);
                        }
                    }

                    //Init data
                    for(Integer houseId : finalHouseDataList.keySet()) {
                        HashMap<String, HouseData> tempFinalHouseData = finalHouseDataList.get(houseId);
                        for(String timeslice : tempFinalHouseData.keySet()){
                            finalHouseDataSize++;
                            HouseData houseData = tempFinalHouseData.get(timeslice);
                            if(!houseData.isSaved()){
                                houseDataNeedSave.push(houseData);
                            }
                            else if((System.currentTimeMillis()-houseData.getLastUpdate())>(2*gap*1000)){
                                houseDataNeedClean.push(houseData);
                            }
                        }
                    }

                    for(String uniqueHouseholdId : finalHouseholdDataList.keySet()) {
                        HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdDataList.get(uniqueHouseholdId);
                        for(String timeslice : tempFinalHouseholdData.keySet()){
                            finalHouseholdDataSize++;
                            HouseholdData householdData = tempFinalHouseholdData.get(timeslice);
                            if(!householdData.isSaved()){
                                householdDataNeedSave.push(householdData);
                            }
                            else if((System.currentTimeMillis()-householdData.getLastUpdate())>(2*gap*1000)){
                                householdDataNeedClean.push(householdData);
                            }
                        }
                    }

                    for(HouseData houseData : houseDataNeedClean){
                        Timeslice timeslice = houseData.getTimeslice();
                        if(timesliceNeedClean.contains(timeslice.getSliceId())) break;
                        for(HouseholdData householdData : householdDataNeedClean){
                            if(timeslice.isSameTimeslice(householdData.getTimeslice())){
                                timesliceNeedClean.push(timeslice.getSliceId());
                                break;
                            }
                        }
                    }

                    // DB store data
                    if(houseDataNeedSave.size()!=0){
                        if(DB_store.pushHouseData(houseDataNeedSave, new File("./tmp/houseData2db-" + gap + ".lck"))){
                            for(HouseData data : houseDataNeedSave){
                                finalHouseDataList.get(data.getHouseId()).get(data.getSliceId()).save();
                            }
                        }
                    }
                    if(householdDataNeedSave.size()!=0){
                        if(DB_store.pushHouseHoldData(householdDataNeedSave, new File("./tmp/householdData2db-" + gap + ".lck"))){
                            for(HouseholdData data : householdDataNeedSave){
                                finalHouseholdDataList.get(data.getHouseholdUniqueId()).get(data.getSliceId()).save();
                            }
                        }
                    }

                    // DB store prop
                    Stack<HouseProp> tempHousePropList = new Stack<HouseProp>();
                    tempHousePropList.addAll(housePropList.values());
                    if(DB_store.pushHouseProp(tempHousePropList, new File("./tmp/houseProp2db-"+ gap +".lck"))){
                        for(HouseProp houseProp : tempHousePropList){
                            housePropList.get(houseProp.getHouseUniqueId()).save();
                        }
                    }
                    Stack<HouseholdProp> tempHouseholdPropList = new Stack<HouseholdProp>();
                    tempHouseholdPropList.addAll(householdPropList.values());
                    if(DB_store.pushHouseholdProp(tempHouseholdPropList, new File("./tmp/householdProp2db-"+ gap +".lck"))){
                        for(HouseholdProp householdProp : tempHouseholdPropList){
                            householdPropList.get(householdProp.getHouseholdUniqueId()).save();
                        }
                    }

                    // Check Outlier
                    for(HouseData houseData : houseDataNeedSave){
                        HouseProp houseProp = housePropList.getOrDefault(houseData.getHouseUniqueId(), new HouseProp(houseData.getHouseId(), houseData.getGap()));
                        // Check min
                        if(config.getHouseCheckMin() && houseProp.getMin()!=0 && (houseProp.getMin() - houseData.getAvg()) <= (houseProp.getMin()*config.getHouseLogGap()/100)){
                            houseNotificationList.push(new HouseNotification(-1, houseData, houseProp));
                        }
                        // Check avg
                        if(config.getHouseCheckAvg() && houseProp.getAvg()!=0 && (houseProp.getAvg() - houseData.getAvg()) >= (houseProp.getAvg()*config.getHouseLogGap()/100)){
                            houseNotificationList.push(new HouseNotification(0, houseData, houseProp));
                        }
                        // Check max
                        if(config.getHouseCheckMax() && houseProp.getMax()!=0 && (houseProp.getMax() - houseData.getAvg()) >= (houseProp.getMax()*config.getHouseLogGap()/100)){
                            houseNotificationList.push(new HouseNotification(0, houseData, houseProp));
                        }
                        housePropList.put(houseData.getHouseUniqueId(), houseProp.addValue(houseData.getAvg()));
                    }

                    for(HouseholdData householdData : householdDataNeedSave){
                        HouseholdProp householdProp = householdPropList.getOrDefault(householdData.getHouseholdUniqueId(), new HouseholdProp(householdData.getHouseId(), householdData.getHouseholdId(), householdData.getGap()));
                        // Check min
                        if(config.getHouseCheckMin() && householdProp.getMin()!=0 && (householdProp.getMin() - householdData.getAvg()) <= (householdProp.getMin()*config.getHouseLogGap()/100)){
                            householdNotificationList.push(new HouseholdNotification(-1, householdData, householdProp));
                        }
                        // Check avg
                        if(config.getHouseCheckAvg() && householdProp.getAvg()!=0 && (householdProp.getAvg() - householdData.getAvg()) >= (householdProp.getAvg()*config.getHouseLogGap()/100)){
                            householdNotificationList.push(new HouseholdNotification(0, householdData, householdProp));
                        }
                        // Check max
                        if(config.getHouseCheckMax() && householdProp.getMax()!=0 && (householdProp.getMax() - householdData.getAvg()) >= (householdProp.getMax()*config.getHouseLogGap()/100)){
                            householdNotificationList.push(new HouseholdNotification(0, householdData, householdProp));
                        }
                        householdPropList.put(householdData.getHouseholdUniqueId(), householdProp.addValue(householdData.getAvg()));
                    }

                    // Publish noti
                    if(config.isNotificationMQTT()){
                        MQTT_publisher.houseNotificationsPublish(houseNotificationList, config.getNotificationBrokerURL(), config.getMqttTopicPrefix());
                        MQTT_publisher.householdNotificationsPublish(householdNotificationList, config.getNotificationBrokerURL(), config.getMqttTopicPrefix());
                    }
                    // Save noti
                    if(DB_store.pushHouseNotification(houseNotificationList, new File("./tmp/housenoti2db-" + gap + ".lck"))){
                        // House noti pushed
                    }
                    if(DB_store.pushHouseholdNotification(householdNotificationList, new File("./tmp/householdnoti2db-" + gap + ".lck"))){
                        // House noti pushed
                    }

                    //Logging
                    Long execTime = System.currentTimeMillis() - startExec;
                    System.out.print(String.format("[Bolt_sum_%-3d] HouseData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n", gap, finalHouseDataSize, houseDataNeedSave.size(), houseDataNeedClean.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] HouseholdData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, finalHouseholdDataSize, householdDataNeedSave.size(), householdDataNeedClean.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] Timeslice | Total: %-10d | Need clean: %-10d\n", gap, allData.size(), timesliceNeedClean.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] Notification | House: %-10d | Household: %-10d\n", gap, houseNotificationList.size(), householdNotificationList.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));

                    Stack<String> stormLogList = new Stack<String>();
                    stormLogList.push(String.format("[Bolt_sum_%-3d] HouseData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n", gap, finalHouseDataSize, houseDataNeedSave.size(), houseDataNeedClean.size()));
                    stormLogList.push(String.format("[Bolt_sum_%-3d] HouseholdData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, finalHouseholdDataSize, householdDataNeedSave.size(), householdDataNeedClean.size()));
                    stormLogList.push(String.format("[Bolt_sum_%-3d] Timeslice | Total: %-10d | Need clean: %-10d\n", gap, allData.size(), timesliceNeedClean.size()));
                    stormLogList.push(String.format("[Bolt_sum_%-3d] Notification | House: %-10d | Household: %-10d\n", gap, houseNotificationList.size(), householdNotificationList.size()));
                    stormLogList.push(String.format("[Bolt_sum_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                    MQTT_publisher.stormLogPublish(stormLogList, config.getNotificationBrokerURL(), config.getMqttTopicPrefix());

                    try {
                        FileWriter log = new FileWriter(new File("tmp/bolt_sum_"+ gap +".tmp"), false);
                        PrintWriter pwOb = new PrintWriter(log , false);
                        pwOb.flush();
                        log.write(String.format("[Bolt_sum_%-3d] HouseData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n", gap, finalHouseDataSize, houseDataNeedSave.size(), houseDataNeedClean.size()));
                        log.write(String.format("[Bolt_sum_%-3d] HouseholdData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, finalHouseholdDataSize, householdDataNeedSave.size(), householdDataNeedClean.size()));
                        log.write(String.format("[Bolt_sum_%-3d] Timeslice | Total: %-10d | Need clean: %-10d\n", gap, allData.size(), timesliceNeedClean.size()));
                        log.write(String.format("[Bolt_sum_%-3d] Notification | House: %-10d | Household: %-10d\n", gap, houseNotificationList.size(), householdNotificationList.size()));
                        log.write(String.format("[Bolt_sum_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                        pwOb.close();
                        log.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                    //Wipe unused data
                    for(String timeslice : timesliceNeedClean){
                        allData.remove(timeslice);
                    }

                    for(HouseData houseData : houseDataNeedClean){
                        finalHouseDataList.get(houseData.getHouseId()).remove(houseData.getSliceId());
                    }

                    for(HouseholdData householdData : householdDataNeedClean) {
                        finalHouseholdDataList.get(householdData.getHouseholdUniqueId()).remove(householdData.getSliceId());
                    }

                }
            }
            else{
                Integer houseId         = (Integer) tuple.getValueByField("houseId");
                Double  avg             = (Double) tuple.getValueByField("avg");
                Integer householdId     = (Integer)tuple.getValueByField("householdId");
                Integer deviceId        = (Integer)tuple.getValueByField("deviceId");
                Integer index           = (Integer)tuple.getValueByField("index");
                String year             = (String)tuple.getValueByField("year");
                String month            = (String)tuple.getValueByField("month");
                String day              = (String)tuple.getValueByField("day");
                DeviceData tempData = new DeviceData(houseId, householdId, deviceId, year, month, day, index, gap).avg(avg).save();

                HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > > sliceData = allData.getOrDefault(tempData.getSliceId(), new HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > >());
                HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.getOrDefault(houseId, new HashMap<Integer,HashMap<String, DeviceData> >());
                HashMap<String, DeviceData> householdData = houseData.getOrDefault(householdId, new HashMap<String, DeviceData>());
                householdData.put(tempData.getUniqueId(), tempData);
                houseData.put(householdId, householdData);
                sliceData.put(houseId, houseData);
                allData.put(tempData.getSliceId(), sliceData);
            }
            _collector.ack(tuple);
        }
        catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(tuple);
        }
    }
}
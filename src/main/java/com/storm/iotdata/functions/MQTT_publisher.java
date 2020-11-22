package com.storm.iotdata.functions;

import java.util.Stack;
import java.util.UUID;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.storm.iotdata.models.*;

public class MQTT_publisher {
    public static void deviceNotificationsPublish(Stack<DeviceNotification> dataList, String brokerURL, String topicPrefix) {
        new DeviceNotificationPublisher(dataList, brokerURL, topicPrefix).start();
    }

    public static void householdNotificationsPublish(Stack<HouseholdNotification> dataList, String brokerURL, String topicPrefix) {
        new HouseholdNotificationPublisher(dataList, brokerURL, topicPrefix).start();
	}

	public static void houseNotificationsPublish(Stack<HouseNotification> dataList, String brokerURL, String topicPrefix) {
        new HouseNotificationPublisher(dataList, brokerURL, topicPrefix).start();
    }
    
    public static void stormLogPublish(Stack<String> dataList, String brokerURL, String topicPrefix) {
        new StormLogPublisher(dataList, brokerURL, topicPrefix).start();
    }
}

class DeviceNotificationPublisher extends Thread {
    String brokerURL = "tcp://mqtt-broker:1883";
    String globalTopic = "%siot-notification";
    String deviceTopic = "%sdevice-%d-%d-%d-notification";
    String householdTopic = "%shousehold-%d-%d-notification";
    String houseTopic = "%shouse-%d-notification";
    String topicPrefix = "";
    Stack<DeviceNotification> dataList;

    public DeviceNotificationPublisher(Stack<DeviceNotification> dataList, String brokerURL, String topicPrefix) {
        this.dataList = dataList;
        this.brokerURL = brokerURL;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void run() {
        Long start = System.currentTimeMillis();
        String publisherId = UUID.randomUUID().toString();
        try {
            IMqttClient publisher = new MqttClient(brokerURL, publisherId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            publisher.connect(options);
            if(publisher.isConnected())
            for(DeviceNotification deviceNotification : dataList){
                byte[] payload = deviceNotification.toString().getBytes();
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(false);
                //Publish to house topic
                publisher.publish(String.format(houseTopic, topicPrefix, deviceNotification.getHouseId()), msg);
                //Publish to household topic
                publisher.publish(String.format(householdTopic, topicPrefix, deviceNotification.getHouseId(), deviceNotification.getHouseholdId()), msg);
                //Publish to device topic
                publisher.publish(String.format(deviceTopic, topicPrefix, deviceNotification.getHouseId(), deviceNotification.getHouseholdId(), deviceNotification.getDeviceId()), msg);
                //Publish to global topic
                publisher.publish(String.format(globalTopic, topicPrefix), msg);
            }
            publisher.disconnect();
            publisher.close();
            System.out.printf("\n[Device Notification Publisher] MQTT Publisher took %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

class HouseholdNotificationPublisher extends Thread {
    String brokerURL = "tcp://mqtt-broker:1883";
    String globalTopic = "%siot-notification";
    String householdTopic = "%shousehold-%d-%d-notification";
    String houseTopic = "%shouse-%d-notification";
    String topicPrefix = "";
    Stack<HouseholdNotification> dataList;

    public HouseholdNotificationPublisher(Stack<HouseholdNotification> dataList, String brokerURL, String topicPrefix) {
        this.dataList = dataList;
        this.brokerURL = brokerURL;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void run() {
        Long start = System.currentTimeMillis();
        String publisherId = UUID.randomUUID().toString();
        try {
            IMqttClient publisher = new MqttClient(brokerURL, publisherId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            publisher.connect(options);
            if(publisher.isConnected())
            for(HouseholdNotification householdNotification : dataList){
                byte[] payload = householdNotification.toString().getBytes();
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(false);
                //Publish to house topic
                publisher.publish(String.format(houseTopic, topicPrefix, householdNotification.getHouseId()), msg);
                //Publish to household topic
                publisher.publish(String.format(householdTopic, topicPrefix, householdNotification.getHouseId(), householdNotification.getHouseholdId()), msg);
                //Publish to global topic
                publisher.publish(String.format(globalTopic, topicPrefix), msg);
            }
            publisher.disconnect();
            publisher.close();
            System.out.printf("\n[Household Notification Publisher] MQTT Publisher took %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

class HouseNotificationPublisher extends Thread {
    String brokerURL = "tcp://mqtt-broker:1883";
    String globalTopic = "%siot-notification";
    String houseTopic = "%shouse-%d-notification";
    String topicPrefix = "";
    Stack<HouseNotification> dataList;

    public HouseNotificationPublisher(Stack<HouseNotification> dataList, String brokerURL, String topicPrefix) {
        this.dataList = dataList;
        this.brokerURL = brokerURL;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void run() {
        Long start = System.currentTimeMillis();
        String publisherId = UUID.randomUUID().toString();
        try {
            IMqttClient publisher = new MqttClient(brokerURL, publisherId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            publisher.connect(options);
            if(publisher.isConnected())
            for(HouseNotification houseNotification : dataList){
                byte[] payload = houseNotification.toString().getBytes();
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(false);
                //Publish to house topic
                publisher.publish(String.format(houseTopic, topicPrefix, houseNotification.getHouseId()), msg);
                //Publish to global topic
                publisher.publish(String.format(globalTopic, topicPrefix), msg);
            }
            publisher.disconnect();
            publisher.close();
            System.out.printf("\n[House Notification Publisher] MQTT Publisher took %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

class StormLogPublisher extends Thread {
    String brokerURL = "tcp://mqtt-broker:1883";
    String stormLogTopic = "%sstorm-log";
    String topicPrefix = "";
    Stack<String> dataList;

    public StormLogPublisher(Stack<String> dataList, String brokerURL, String topicPrefix) {
        this.dataList = dataList;
        this.brokerURL = brokerURL;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void run() {
        Long start = System.currentTimeMillis();
        String publisherId = UUID.randomUUID().toString();
        try {
            IMqttClient publisher = new MqttClient(brokerURL, publisherId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            publisher.connect(options);
            if(publisher.isConnected())
            for(String log : dataList){
                byte[] payload = log.getBytes();
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(true);
                publisher.publish(String.format(stormLogTopic, topicPrefix), msg);
            }
            publisher.disconnect();
            publisher.close();
            System.out.printf("\n[Storm Log Publisher] MQTT Publisher took %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
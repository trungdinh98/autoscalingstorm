package com.storm.iotdata.models;

import com.google.gson.Gson;

public class HouseNotification extends Timeslice{
    public Integer type;
    public Integer houseId;
    public Double min;
    public Double max;
    public Double avg;
    public Double value;
    public Long timestamp;
    public Boolean saved=false;

    public HouseNotification(Integer type, HouseData data, HouseProp dataProp){
        super(data.year, data.month, data.day, data.sliceIndex, data.sliceGap);
        this.type=type;
        this.houseId=data.houseId;
        this.value=data.getAvg();
        this.min=dataProp.min;
        this.max=dataProp.max;
        this.avg=dataProp.avg;
        this.timestamp=System.currentTimeMillis();
    }

    public Integer getType() {
        return this.type;
    }

    public Integer getHouseId() {
        return this.houseId;
    }

    public Double getMin() {
        return this.min;
    }

    public Double getMax() {
        return this.max;
    }

    public Double getAvg() {
        return this.avg;
    }

    public Double getValue() {
        return this.value;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public Boolean getSaved() {
        return this.saved;
    }

    public Boolean isSaved() {
        return this.saved;
    }

    public String toString() {
        Gson gson=new Gson();    
        return gson.toJson(this);
    }
}

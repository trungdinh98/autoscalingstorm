package com.storm.iotdata.models;

import java.util.Objects;

public class Timeslice{
    public String year;
    public String month;
    public String day;
    public Integer sliceIndex;
    public Integer sliceGap;

    public Timeslice(Timeslice timeslice){
        this.year = timeslice.getYear();
        this.month = timeslice.getMonth();
        this.day = timeslice.getDay();
        this.sliceIndex = timeslice.getIndex();
        this.sliceGap = timeslice.getGap();
    }

    public Timeslice(String sliceId) {
        try{
            String[] sliceProp = sliceId.split("-");
            this.year = sliceProp[0];
            this.month = sliceProp[1];
            this.day = sliceProp[2];
            this.sliceIndex = Integer.parseInt(sliceProp[3]);
            this.sliceGap = Integer.parseInt(sliceProp[4]);
        } catch (Exception ex) {
            throw ex;
        }
    }

    public Timeslice(String year, String month, String day, Integer index, Integer gap) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.sliceIndex = index;
        this.sliceGap = gap;
    }

    public String getYear() {
        return this.year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return this.month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return this.day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getIndex() {
        return this.sliceIndex;
    }

    public void setIndex(Integer index) {
        this.sliceIndex = index;
    }

    public Integer getGap() {
        return this.sliceGap;
    }

    public void setGap(Integer gap) {
        this.sliceGap = gap;
    }

    public Timeslice year(String year) {
        this.year = year;
        return this;
    }

    public Timeslice month(String month) {
        this.month = month;
        return this;
    }

    public Timeslice day(String day) {
        this.day = day;
        return this;
    }

    public Timeslice index(Integer index) {
        this.sliceIndex = index;
        return this;
    }

    public Timeslice gap(Integer gap) {
        this.sliceGap = gap;
        return this;
    }

    public Boolean isSameTimeslice(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Timeslice)) {
            return false;
        }
        Timeslice timeslice = (Timeslice) o;
        return Objects.equals(year, timeslice.year) && Objects.equals(month, timeslice.month) && Objects.equals(day, timeslice.day) && Objects.equals(sliceIndex, timeslice.sliceIndex) && Objects.equals(sliceGap, timeslice.sliceGap);
    }

    public Timeslice getTimeslice(){
        return new Timeslice(year, month, day, sliceIndex, sliceGap);
    }

    public String getSliceId() {
        return year + "-" + month + "-" + day + "-" + sliceIndex + "-" + sliceGap;
    }

    public String getSliceName() {
        return year + "/" + month + "/" + day + " " +  String.format("%02d", Math.floorDiv((sliceIndex*sliceGap),60)) + ":" +  String.format("%02d", (sliceIndex*sliceGap)%60) + "->" +  String.format("%02d", Math.floorDiv(((sliceIndex+1)*sliceGap),60)) + ":" +  String.format("%02d", ((sliceIndex+1)*sliceGap)%60) ;
    }

    public String getDate() {
        return year + "/" + month + "/" + day;
    }
    
}

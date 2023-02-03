package com.xiaozhu.data.window;

public class TempRecord {
    private String province;
    private String city;
    private Double temp;
    private long timeEpochMilli;
    public String getProvince() {
        return this.province;
    }

    public String getCity() {
        return this.city;
    }

    public Double getTemp() {
        return this.temp;
    }

    public long getTimeEpochMilli() {
        return this.timeEpochMilli;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    public void setTimeEpochMilli(long timeEpochMilli) {
        this.timeEpochMilli = timeEpochMilli;
    }

    @Override
    public String toString() {
        return "TempRecord{" +
                "province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", temp=" + temp +
                ", timeEpochMilli=" + timeEpochMilli +
                '}';
    }
}

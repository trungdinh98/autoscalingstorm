package com.storm.iotdata.functions;

import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.storm.iotdata.models.*;

import org.yaml.snakeyaml.Yaml;

public class DB_store {

    private Connection conn;

    private static InputStream getConfig() {
        return DB_store.class.getClassLoader().getResourceAsStream("config/cred.yaml");
    }

    public static Connection initConnection() throws ClassNotFoundException, SQLException, FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = getConfig();
        Map<String, Object> obj = yaml.load(inputStream);
        String dbURL = "jdbc:mysql://" + obj.get("db_url");
        String userName = (String) obj.get("db_user");
        String password = (String) obj.get("db_pass");
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(dbURL, userName, password);
    }

    public DB_store() {
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = getConfig();
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, userName, password);
        } catch (SQLException sql) {
            System.out.println("SQLException: " + sql.getMessage());
            System.out.println("SQLState: " + sql.getSQLState());
            System.out.println("Erro: " + sql.getErrorCode());
            System.out.println("StackTrace: " + sql.getStackTrace());
        } catch (Exception ex) {
            System.out.println("connect failure!");
            ex.printStackTrace();
        }
    }

    public static boolean purgeData() {
        try {
            Connection conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate("drop database iot_data");
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            if (DB_store.initData())
                return true;
            else
                return false;
        }
    }

    public static boolean initData() {
        try {
            Connection conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            try {
                stmt.executeUpdate("create database iot_data");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            stmt.execute("use iot_data");
            try {
                stmt.executeUpdate(
                        "create table device_data (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table household_data(house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table house_data(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table house_data_forecast(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table forecast_meta_data(version VARCHAR(4) NOT NULL, slice_gap INT UNSIGNED NOT NULL, count DOUBLE UNSIGNED DEFAULT 0, mean DOUBLE UNSIGNED DEFAULT 0, variance DOUBLE UNSIGNED DEFAULT 0, standart_deviation DOUBLE UNSIGNED DEFAULT 0, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(slice_gap, version))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table device_prop (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, slice_gap INT UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, slice_gap))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table device_notification (type INT SIGNED NOT NULL, house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(type, house_id, household_id, device_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table household_prop (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, slice_gap INT UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, slice_gap))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table household_notification (type INT SIGNED NOT NULL, house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(type, house_id, household_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table house_prop (house_id INT UNSIGNED NOT NULL, slice_gap INT UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, slice_gap))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table house_notification (type INT SIGNED NOT NULL, house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(type, house_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean initForecastTable(String table) {
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = getConfig();
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            stmt.execute("drop table if exists " + table);
            stmt.executeUpdate("create table " + table
                    + "(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseData(Stack<HouseData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseHoldData(Stack<HouseholdData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean pushForecastHouseData(Stack<HouseData> dataList) {
        try {
            // Init SQL
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            String sql = "insert into house_data_forecast (house_id,year,month,day,slice_gap,slice_index,avg) values ";
            for (HouseData data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement("(?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setString(2, data.getYear());
                tempSql.setString(3, data.getMonth());
                tempSql.setString(4, data.getDay());
                tempSql.setInt(5, data.getGap());
                tempSql.setInt(6, data.getIndex());
                tempSql.setDouble(7, data.getValue());
                String statementText = tempSql.toString();
                sql += statementText.substring(statementText.indexOf(": ") + 2) + ",";
            }
            sql = sql.substring(0, sql.length() - 1) + " on duplicate key update avg=VALUES(avg)";
            stmt.executeUpdate(sql);
            conn.close();
            // System.out.printf("\nDB tooks %.2f
            // s\n",(float)(System.currentTimeMillis()-start)/1000);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean pushForecastHouseData(HouseData data, String table) {
        String sql = "insert into " + table + " (house_id,year,month,day,slice_gap,slice_index,avg) values ";
        try {
            // Init SQL
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            PreparedStatement tempSql = this.conn.prepareStatement("(?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            tempSql.setInt(1, data.getHouseId());
            tempSql.setString(2, data.getYear());
            tempSql.setString(3, data.getMonth());
            tempSql.setString(4, data.getDay());
            tempSql.setInt(5, data.getGap());
            tempSql.setInt(6, data.getIndex());
            tempSql.setDouble(7, data.getValue());
            String statementText = tempSql.toString();
            sql += statementText.substring(statementText.indexOf(": ") + 2) + ",";
            sql = sql.substring(0, sql.length() - 1) + " on duplicate key update avg=VALUES(avg)";
            stmt.executeUpdate(sql);
            // System.out.printf("\nDB tooks %.2f
            // s\n",(float)(System.currentTimeMillis()-start)/1000);
            return true;
        } catch (Exception ex) {
            System.out.println(sql);
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return pushForecastHouseData(data, table);
        }
    }

    public static boolean pushDeviceData(Stack<DeviceData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushDeviceNotification(Stack<DeviceNotification> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceNotification2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseholdNotification(Stack<HouseholdNotification> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdNotification2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseNotification(Stack<HouseNotification> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseNotification2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean saveData(DeviceData data) {
        try {
            // Init SQL
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            PreparedStatement tempSql = conn.prepareStatement(
                    "insert into device_data (house_id,household_id,device_id,year,month,day,slice_gap,slice_index,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), count=VALUES(count), avg=VALUES(avg)",
                    Statement.RETURN_GENERATED_KEYS);
            tempSql.setInt(1, data.getHouseId());
            tempSql.setInt(2, data.getHouseholdId());
            tempSql.setInt(3, data.getDeviceId());
            tempSql.setString(4, data.getYear());
            tempSql.setString(5, data.getMonth());
            tempSql.setString(6, data.getDay());
            tempSql.setInt(7, data.getGap());
            tempSql.setInt(8, data.getIndex());
            tempSql.setDouble(9, data.getValue());
            tempSql.setDouble(10, data.getCount());
            tempSql.setDouble(11, data.getAvg());
            tempSql.executeUpdate();
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    };

    public Stack<HouseData> query(int house_id, String year, String month, String day, int slice_gap, int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "SELECT * FROM house_data WHERE ";
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            Boolean condition = false;
            if (house_id != -1) {
                sql += "house_id=" + house_id + " AND ";
                condition = true;
            }
            if (year.length() != 0) {
                sql += "year=\"" + year + "\" AND ";
                condition = true;
            }
            if (month.length() != 0) {
                sql += "month=\"" + month + "\" AND ";
                condition = true;
            }
            if (day.length() != 0) {
                sql += "day=\"" + day + "\" AND ";
                condition = true;
            }
            if (house_id != -1) {
                sql += "slice_gap=" + slice_gap + " AND ";
                condition = true;
            }
            if (slice_index != -1) {
                sql += "slice_index=" + slice_index + " AND ";
                condition = true;
            }
            try (ResultSet rs = stmt.executeQuery(sql.substring(0, sql.length() - (condition ? 5 : 7)))) {
                while (rs.next()) {
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            this.reConnect();
            System.out.println("[ERROR] " + sql);
            return query(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV0(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND year=\"" + year + "\" AND month=\""
                + month + "\" AND day=\"" + day + "\" AND slice_gap=" + slice_gap;
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .after(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .equals(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        if (rs.getInt("slice_index") > slice_index) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV0(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV1(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND slice_gap=" + slice_gap;
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .after(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .equals(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        if (rs.getInt("slice_index") > slice_index) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV1(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV2(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "";
        try {
            Boolean end = false;
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            Calendar query_cal = Calendar.getInstance();
            query_cal.setTime(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1, Integer.valueOf(day)));
            do {
                sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND year=\""
                        + query_cal.get(Calendar.YEAR) + "\" AND month=\""
                        + String.format("%02d", (query_cal.get(Calendar.MONTH) + 1)) + "\" AND day=\""
                        + String.format("%02d", query_cal.get(Calendar.DAY_OF_MONTH)) + "\" AND slice_gap=" + slice_gap
                        + " AND slice_index=" + slice_index;
                query_cal.add(Calendar.WEEK_OF_YEAR, -1);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                                rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                                rs.getDouble("avg")));
                    } else {
                        end = true;
                    }
                }
            } while (!end);
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV2(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV3(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND slice_index=" + slice_index
                + " AND slice_gap=" + slice_gap;
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .after(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .equals(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        if (rs.getInt("slice_index") > slice_index) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV3(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public void reConnect() {
        try {
            this.conn.close();
            // Init connection
            Yaml yaml = new Yaml();
            InputStream inputStream = getConfig();
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.conn = DriverManager.getConnection(dbURL, userName, password);
        } catch (Exception ex) {
            System.out.println("connect failure! Retrying...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            this.reConnect();
            ex.printStackTrace();
        }
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static HashMap<String, DeviceProp> initDevicePropList() {
        Connection conn;
        HashMap<String, DeviceProp> result = new HashMap<String, DeviceProp>();
        String sql = "SELECT * FROM device_prop";
        try {
            conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    DeviceProp tempDeviceProp = new DeviceProp(rs.getInt("house_id"), rs.getInt("household_id"), rs.getInt("device_id"), rs.getInt("slice_gap"), rs.getDouble("min"), rs.getDouble("avg"), rs.getDouble("max"), rs.getDouble("count"), true);
                    result.put(tempDeviceProp.getDeviceUniqueId(), tempDeviceProp);
                }
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return new HashMap<String, DeviceProp>();
        }
	}

	public static boolean pushDeviceProp(Stack<DeviceProp> dataList, File locker) {
		try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceProp2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

	public static HashMap<String, HouseholdProp> initHouseholdPropList() {
		Connection conn;
        HashMap<String, HouseholdProp> result = new HashMap<String, HouseholdProp>();
        String sql = "SELECT * FROM household_prop";
        try {
            conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    HouseholdProp tempHouseholdProp = new HouseholdProp(rs.getInt("house_id"), rs.getInt("household_id"), rs.getInt("slice_gap"), rs.getDouble("min"), rs.getDouble("avg"), rs.getDouble("max"), rs.getDouble("count"), true);
                    result.put(tempHouseholdProp.getHouseholdUniqueId(), tempHouseholdProp);
                }
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return new HashMap<String, HouseholdProp>();
        }
    }
    
    public static boolean pushHouseholdProp(Stack<HouseholdProp> dataList, File locker) {
		try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdProp2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
	}

	public static HashMap<String, HouseProp> initHousePropList() {
		Connection conn;
        HashMap<String, HouseProp> result = new HashMap<String, HouseProp>();
        String sql = "SELECT * FROM house_prop";
        try {
            conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    HouseProp tempHouseProp = new HouseProp(rs.getInt("house_id"), rs.getInt("slice_gap"), rs.getDouble("min"), rs.getDouble("avg"), rs.getDouble("max"), rs.getDouble("count"), true);
                    result.put(tempHouseProp.getHouseUniqueId(), tempHouseProp);
                }
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return new HashMap<String, HouseProp>();
        }
    }

	public static boolean pushHouseProp(Stack<HouseProp> dataList, File locker) {
		try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseProp2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
	}
    
}

class DeviceProp2DB extends Thread {
    private Stack<DeviceProp> dataList;
    private File locker;
    
    public DeviceProp2DB(Stack<DeviceProp> dataList, File locker){
        this.dataList=dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (DeviceProp data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement(
                        "insert into device_prop (house_id,household_id,device_id,slice_gap,min,avg,max,count) values (?,?,?,?,?,?,?,?) on duplicate key update min=VALUES(min), avg=VALUES(avg), max=VALUES(max), count=VALUES(count)",
                        Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getDeviceId());
                tempSql.setInt(4, data.getSliceGap());
                tempSql.setDouble(5, data.getMin());
                tempSql.setDouble(6, data.getAvg());
                tempSql.setDouble(7, data.getMax());
                tempSql.setDouble(8, data.getCount());
                tempSql.executeUpdate();
            }
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class HouseholdProp2DB extends Thread {
    private Stack<HouseholdProp> dataList;
    private File locker;
    
    public HouseholdProp2DB(Stack<HouseholdProp> dataList, File locker){
        this.dataList=dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (HouseholdProp data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement(
                        "insert into household_prop (house_id,household_id,slice_gap,min,avg,max,count) values (?,?,?,?,?,?,?) on duplicate key update min=VALUES(min), avg=VALUES(avg), max=VALUES(max), count=VALUES(count)",
                        Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getSliceGap());
                tempSql.setDouble(4, data.getMin());
                tempSql.setDouble(5, data.getAvg());
                tempSql.setDouble(6, data.getMax());
                tempSql.setDouble(7, data.getCount());
                tempSql.executeUpdate();
            }
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class HouseProp2DB extends Thread {
    private Stack<HouseProp> dataList;
    private File locker;
    
    public HouseProp2DB(Stack<HouseProp> dataList, File locker){
        this.dataList=dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (HouseProp data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement(
                        "insert into house_prop (house_id,slice_gap,min,avg,max,count) values (?,?,?,?,?,?) on duplicate key update min=VALUES(min), avg=VALUES(avg), max=VALUES(max), count=VALUES(count)",
                        Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getSliceGap());
                tempSql.setDouble(3, data.getMin());
                tempSql.setDouble(4, data.getAvg());
                tempSql.setDouble(5, data.getMax());
                tempSql.setDouble(6, data.getCount());
                tempSql.executeUpdate();
            }
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class DeviceData2DB extends Thread {
    private Stack<DeviceData> dataList;
    private File locker;
    
    public DeviceData2DB(Stack<DeviceData> dataList, File locker){
        this.dataList=dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (DeviceData data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement(
                        "insert into device_data (house_id,household_id,device_id,year,month,day,slice_gap,slice_index,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), count=VALUES(count), avg=VALUES(avg)",
                        Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getDeviceId());
                tempSql.setString(4, data.getYear());
                tempSql.setString(5, data.getMonth());
                tempSql.setString(6, data.getDay());
                tempSql.setInt(7, data.getGap());
                tempSql.setInt(8, data.getIndex());
                tempSql.setDouble(9, data.getValue());
                tempSql.setDouble(10, data.getCount());
                tempSql.setDouble(11, data.getAvg());
                tempSql.executeUpdate();
            }
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class HouseholdData2DB extends Thread {
    private Stack<HouseholdData> dataList;
    private File locker;
    
    public HouseholdData2DB(Stack<HouseholdData> dataList, File locker){
        this.dataList=dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (HouseholdData data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement(
                        "insert into household_data (house_id,household_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)",
                        Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setString(3, data.getYear());
                tempSql.setString(4, data.getMonth());
                tempSql.setString(5, data.getDay());
                tempSql.setInt(6, data.getGap());
                tempSql.setInt(7, data.getIndex());
                tempSql.setDouble(8, data.getValue());
                tempSql.executeUpdate();
            }
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class HouseData2DB extends Thread {
    private Stack<HouseData> dataList;
    private File locker;
    
    public HouseData2DB(Stack<HouseData> dataList, File locker){
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (HouseData data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement("insert into house_data (house_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)", Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getHouseId());
                tempSql.setString(2, data.getYear());
                tempSql.setString(3, data.getMonth());
                tempSql.setString(4, data.getDay());
                tempSql.setInt(5, data.getGap());
                tempSql.setInt(6, data.getIndex());
                tempSql.setDouble(7, data.getValue());
                tempSql.executeUpdate();
                // String statementText = tempSql.toString();
                // sql += statementText.substring(statementText.slice_indexOf(": ") + 2) + ",";
            }
            // sql = sql.substring(0, sql.length() - 1) + "";
            // stmt.executeUpdate(sql);
            conn.close();
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class DeviceNotification2DB extends Thread {
    private Stack<DeviceNotification> dataList;
    private File locker;

    public DeviceNotification2DB(Stack<DeviceNotification> dataList, File locker){
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (DeviceNotification data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement("insert into device_notification (type,house_id,household_id,device_id,year,month,day,slice_gap,slice_index,value,min,max,avg) values (?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), min=VALUES(min), max=VALUES(max), avg=VALUES(avg)", Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getType());
                tempSql.setInt(2, data.getHouseId());
                tempSql.setInt(3, data.getHouseholdId());
                tempSql.setInt(4, data.getDeviceId());
                tempSql.setString(5, data.getYear());
                tempSql.setString(6, data.getMonth());
                tempSql.setString(7, data.getDay());
                tempSql.setInt(8, data.getGap());
                tempSql.setInt(9, data.getIndex());
                tempSql.setDouble(10, data.getValue());
                tempSql.setDouble(11, data.getMin());
                tempSql.setDouble(12, data.getMax());
                tempSql.setDouble(13, data.getAvg());
                tempSql.executeUpdate();
            }
            conn.close();
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class HouseholdNotification2DB extends Thread {
    private Stack<HouseholdNotification> dataList;
    private File locker;

    public HouseholdNotification2DB(Stack<HouseholdNotification> dataList, File locker){
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (HouseholdNotification data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement("insert into household_notification (type,house_id,household_id,year,month,day,slice_gap,slice_index,value,min,max,avg) values (?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), min=VALUES(min), max=VALUES(max), avg=VALUES(avg)", Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getType());
                tempSql.setInt(2, data.getHouseId());
                tempSql.setInt(3, data.getHouseholdId());
                tempSql.setString(4, data.getYear());
                tempSql.setString(5, data.getMonth());
                tempSql.setString(6, data.getDay());
                tempSql.setInt(7, data.getGap());
                tempSql.setInt(8, data.getIndex());
                tempSql.setDouble(9, data.getValue());
                tempSql.setDouble(10, data.getMin());
                tempSql.setDouble(11, data.getMax());
                tempSql.setDouble(12, data.getAvg());
                tempSql.executeUpdate();
            }
            conn.close();
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}

class HouseNotification2DB extends Thread {
    private Stack<HouseNotification> dataList;
    private File locker;

    public HouseNotification2DB(Stack<HouseNotification> dataList, File locker){
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            // Init connection
            Connection conn = DB_store.initConnection();
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (HouseNotification data : dataList) {
                PreparedStatement tempSql = conn.prepareStatement("insert into house_notification (type,house_id,year,month,day,slice_gap,slice_index,value,min,max,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), min=VALUES(min), max=VALUES(max), avg=VALUES(avg)", Statement.RETURN_GENERATED_KEYS);
                tempSql.setInt(1, data.getType());
                tempSql.setInt(2, data.getHouseId());
                tempSql.setString(3, data.getYear());
                tempSql.setString(4, data.getMonth());
                tempSql.setString(5, data.getDay());
                tempSql.setInt(6, data.getGap());
                tempSql.setInt(7, data.getIndex());
                tempSql.setDouble(8, data.getValue());
                tempSql.setDouble(9, data.getMin());
                tempSql.setDouble(10, data.getMax());
                tempSql.setDouble(11, data.getAvg());
                tempSql.executeUpdate();
            }
            conn.close();
            System.out.printf("\n["+ locker.getName() +"] DB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            locker.delete();
        }
    }
}
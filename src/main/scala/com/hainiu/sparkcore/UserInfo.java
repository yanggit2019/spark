package com.hainiu.sparkcore;

import org.apache.hadoop.io.Text;

import java.io.Serializable;

public class UserInfo implements Serializable {
    private String name = "hainiu"; // java实现了序列化

    private int age = 10;  // java实现了序列化

//    private static Text addr = new Text("beijing");  // 没有实现java的 Serializable接口
    private transient Text addr = new Text("beijing");  // 没有实现java的 Serializable接口

    public UserInfo() {
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", addr=" + addr +
                '}';
    }
}

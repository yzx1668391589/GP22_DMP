package com.Redis;

import redis.clients.jedis.Jedis;

import java.io.*;

public class RedisDemo {
    public static void main(String[] args) {
        Jedis jedis =new Jedis("hadoop01",6379);

        try { // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw  

            /* 读入TXT文件 */
            String pathname = args[0]; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径
            File filename = new File(pathname); // 要读取以上路径的input。txt文件
            InputStreamReader reader = new InputStreamReader(
                    new FileInputStream(filename)); // 建立一个输入流对象reader
            BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
            String line = "";
            int i=0;
            line = br.readLine();
            while (line != null) {
                line = br.readLine(); // 一次读入一行数据
                String[] str = line.replace(" ", "-").split("\t");
                if(str.length>5){
                if (str[3].contains(".")) {
                    jedis.set(str[3],str[1]);
                } else if (str[4].contains(".")) {
                    jedis.set(str[4],str[1]);
                } else {
                    jedis.set(str[5],str[1]);
                }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

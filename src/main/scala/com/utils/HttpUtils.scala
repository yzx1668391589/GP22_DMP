package com.utils

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils


object HttpUtils {

  def get(url:String):String={
    val httpClient = HttpClients.createDefault()    // 创建 client 实例
    val get = new HttpGet(url)    // 创建 get 实例

    val response = httpClient.execute(get)    // 发送请求
    EntityUtils.toString(response.getEntity,"UTF-8")    // 获取返回结果

  }
}

package com.utils

/**
  * 打标签的统一接口
  */
trait Tag {
  def makeTags(args:Any*):List[(String,Int)]

}

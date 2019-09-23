package com.util.tagutil

trait Tag {
  def makeTags(args:Any*):List[(String,Int)]
}

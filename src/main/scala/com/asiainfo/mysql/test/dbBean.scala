package com.asiainfo.mysql.test

import scala.beans.BeanProperty

class dbBean {
  @BeanProperty var activity_id: Int = _
  @BeanProperty var order_id: Int = _
  @BeanProperty var material_id: Int = _
  @BeanProperty var ad_id: Int = _
  @BeanProperty var size_id: Int = _
  @BeanProperty var area_id: Int = _
  @BeanProperty var start_time: String = "2014-07-24 18:00:00"
  @BeanProperty var end_time: String = "2014-07-24 18:14:59"
  @BeanProperty var media: String = "mediamediamedia"
  @BeanProperty var ad_pos_id: String = "ad_pos_id"
  @BeanProperty var bid_cnt: Int = _
  @BeanProperty var bid_success_cnt: Int = _
  @BeanProperty var expose_cnt: Int = _
  @BeanProperty var click_cnt: Int = _
  @BeanProperty var arrive_cnt: Int = _
  @BeanProperty var second_jump_cnt: Int = _
  @BeanProperty var trans_cnt: Int = _
  @BeanProperty var cost: Int = _
  
  override def toString = {
    "activity_id," +
      "order_id," +
      "material_id," +
      "ad_id," +
      "size_id," +
      "area_id," +
      "start_time," +
      "end_time," +
      "media," +
      "ad_pos_id," +
      "bid_cnt," +
      "bid_success_cnt," +
      "expose_cnt," +
      "click_cnt," +
      "arrive_cnt," +
      "second_jump_cnt," +
      "trans_cnt," +
      "cost"
  }
}
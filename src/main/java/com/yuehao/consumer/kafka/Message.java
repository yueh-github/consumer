package com.yuehao.consumer.kafka;

import java.util.Date;

/**
 * Created by yuehao on 2017/8/24.
 */
public class Message {

    private Long id;

    private String msg;

    private Date date;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}

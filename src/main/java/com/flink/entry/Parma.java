package com.flink.entry;

import java.io.Serializable;

/**
 * Created by jaryzhen on 6/3/16.
 */
public class Parma implements Serializable {

    private String search;
    private String from;
    private Long logsize;
    private String uid;
    private Integer within;
    private String[] transaction;

    private String contains;

    public String getContains() {
        return contains;
    }

    public void setContains(String contains) {
        this.contains = contains;
    }

    public Long getLogsize() {
        return logsize;
    }

    public void setLogsize(Long logsize) {
        this.logsize = logsize;
    }

    public String[] getTransaction() {
        return transaction;
    }

    public void setTransaction(String[] transaction) {
        this.transaction = transaction;
    }


    public String getSearch() {
        return search;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }


    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Integer getWithin() {
        return within;
    }

    public void setWithin(Integer within) {
        this.within = within;
    }
}

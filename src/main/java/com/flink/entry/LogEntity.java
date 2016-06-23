package com.flink.entry;


import org.hibernate.annotations.Type;

import javax.persistence.*;

/**
 * Created by jaryzhen on 5/25/16.
 */
@Entity
public class LogEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)

    private Long id;

    private String index;
    private String type;
    private String score;
    private String gl2_remote_ip;
    private String gl2_remote_port;
    private String gl2_source_node;
    private String source;
    @Lob
    @Column(length = 10000)
    private String message;
    private String gl2_source_input;
    private String timestamp;

    private boolean deleted = false;

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }


    public String getScore() {
        return score;
    }

    public String getGl2_remote_ip() {
        return gl2_remote_ip;
    }

    public String getGl2_remote_port() {
        return gl2_remote_port;
    }

    public String getGl2_source_node() {
        return gl2_source_node;
    }

    public String getSource() {
        return source;
    }

    public String getMessage() {
        return message;
    }

    public String getGl2_source_input() {
        return gl2_source_input;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public void setGl2_remote_ip(String gl2_remote_ip) {
        this.gl2_remote_ip = gl2_remote_ip;
    }

    public void setGl2_remote_port(String gl2_remote_port) {
        this.gl2_remote_port = gl2_remote_port;
    }

    public void setGl2_source_node(String gl2_source_node) {
        this.gl2_source_node = gl2_source_node;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setGl2_source_input(String gl2_source_input) {
        this.gl2_source_input = gl2_source_input;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}

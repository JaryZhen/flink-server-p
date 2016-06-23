package com.flink.util;

import org.apache.log4j.Logger;

/**
 * Created by jaryzhen on 6/19/16.
 */
public class FlinkServerLog {

    protected static final Logger MSG_LOG = Logger.getLogger("msg_log");
    protected static final Logger EXCEPTION_LOG  = Logger
            .getLogger("exception");

    private static final int      BUFFER_SIZE    = 1024;

    public final static int       ERROR_LOG      = 1;
    public final static int       ADMIN_LOG      = 2;
    public final static int       CLIENT_LOG     = 3;


    public static void MsgLog(String msg)
    {
        if (msg != null)
        {
            MSG_LOG.info(msg);
        }
    }
}

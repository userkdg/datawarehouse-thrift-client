package com.terry.impala.thrift;

import com.terry.impala.thrift.hive.QueryInstance;

import java.io.IOException;

public class HiveThriftClientTest {
    public static void main(String[] args) throws IOException {
        try {

            QueryInstance base = new QueryInstance();

            base.submitQuery2("show tables");
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}
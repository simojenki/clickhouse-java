package com.clickhouse.client.http;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;

import java.time.Instant;
import java.util.HashMap;

import static java.lang.Thread.sleep;

public class LoadBalancerExample {

    /**
     * This example seems to produce unexpected results.
     * To reproduce;
     *  - start up clickhouse-examples/docker-compose-recipes/recipes/cluster_1S_2R
     *  - shut down clickhouse-01. ie. docker kill clickhouse-01
     *  - Run this main method.
     *
     * Expected outcome is that when we get a node using nodes.apply(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP)) below
     * that node is both available, and used for the request, expected stdout from this main method would be;
     *  many lines like 'check: nodes:[localhost:8124], faulty:[localhost:8123]' as 8123 is a faulty node as is down, so should never be queried
     *  many progress '.'
     *  no ERRORs
     *
     *
     *  Actual outcome:
     *  For some reason ClickHouseNodes flips between 8123, 8124 being faulty, even though only 8123 has been stopped, and 8124 is running the entire time.
     *  Summary of logs is;
     *
     *  For some reason 8124 is also deemed faulty sometimes; ie.
     *    2023-12-19T09:17:03.310083654Z check: nodes:[localhost:8123], faulty:[localhost:8124]
     *  Sometimes both are fault; ie.
     *    2023-12-19T09:17:03.351453583Z check: nodes:[], faulty:[localhost:8124, localhost:8123]
     *  Sometimes none are faulty; ie.
     *    2023-12-19T09:17:03.340444470Z check: nodes:[localhost:8124, localhost:8123], faulty:[]
     *
     *  Also sometimes we get a node below using nodes.apply(...), however when the actual client is created we instead get;
     *  the other host;
     *  ie in the logs it shows up like;.
     *    ??? asked node:http://localhost:8124/ for a node, got:http://localhost:8123/
     *    2023-12-19T09:17:03.532582686Z ERROR trying to query:http://localhost:8124/, however client is pointing at:Agent{client=ClickHouseHttpClient{http://localhost:8123/}}
     *
     *
     *  All of this seems to happen because executing the request ends up invoking
     *    ClickHouseNode.apply, which then re-selects a node from the 'manager', ie. ClickHouseNodes, which returns a different node that the one that was actually requested to execute
     *
     *
     *  Interestingly, uncommenting ClickHouseNode#1199 seems to resolve the issue, we get no errors, and 8123 also always faulty, no flapping about
     */
    public static void main(String[] args) throws Exception {
        ClickHouseNodes nodes = ClickHouseNodes.of("http://localhost:8123,localhost:8124", new HashMap<>() {{
            put(ClickHouseClientOption.LOAD_BALANCING_POLICY.getKey(), "random");
            put(ClickHouseClientOption.HEALTH_CHECK_INTERVAL.getKey(), "1000");
            put(ClickHouseClientOption.CHECK_ALL_NODES.getKey(), "true");
            put(ClickHouseClientOption.AUTO_DISCOVERY.getKey(), "false");
        }});
/*
        while (true) {
            ClickHouseNode node = nodes.apply(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP));
            try (ClickHouseClient client = ClickHouseClient.newInstance(node.getProtocol())) {
                try (ClickHouseResponse response = client.read(node)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select 1")
                        .execute()
                        .get()) {
                    if (response.firstRecord().getValue(0).asInteger() != 1) {
                        System.err.println(Instant.now() + " " + "ERROR, didn't get 1 back for some reason....");
                        System.exit(1);
                    }
                } catch (Exception e) {
                    System.err.println(Instant.now() + " ERROR:" + e.getMessage());
                }
            }
            sleep(1000);
            System.out.print(".");
        }
*/

        while (true) {
            try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
                try (ClickHouseResponse response = client.read(nodes)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select 1")
                        .execute()
                        .get()) {
                    if (response.firstRecord().getValue(0).asInteger() != 1) {
                        System.err.println(Instant.now() + " " + "ERROR, didn't get 1 back for some reason....");
                        System.exit(1);
                    }
                } catch (Exception e) {
                    System.err.println(Instant.now() + " ERROR:" + e.getMessage());
                }
            }
            sleep(1000);
            System.out.print(".");
        }
    }
}

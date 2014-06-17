package com.mytest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by yarui.zhou on 2014/6/16.
 */
public class Mapping {


    public static void main(String[] args) throws Exception {
        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("10.19.2.182", 9300))
                .addTransportAddress(new InetSocketTransportAddress("10.19.2.181", 9300));

//        XContentBuilder content = XContentFactory.jsonBuilder()
//                .startObject()
//                .startObject("_all")
//                .field("type", "string")
//                .field("indexAnalyzer", "ik")
//                .field("searchAnalyzer", "ik")
//                .endObject()
//                .endObject();
        XContentBuilder content2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("page")
                    .startObject("properties")
                        .startObject("title")
                            .field("type", "string")
                            .field("indexAnalyzer", "ik")
                            .field("searchAnalyzer", "ik")
                        .endObject()
                        .startObject("code")
                            .field("type", "string")
                            .field("indexAnalyzer", "ik")
                            .field("searchAnalyzer", "ik")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject();
        client.close();
    }
}

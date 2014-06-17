package com.mytest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by yarui.zhou on 2014/6/13.
 */
public class QueryData {


    public static void main(String[] args) throws Exception {
        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("10.19.2.182", 9300))
                .addTransportAddress(new InetSocketTransportAddress("10.19.2.181", 9300));


        SearchResponse response = client.prepareSearch("aospoi")
                .setTypes("poi")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("poiTypeId","071600"))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        System.out.println(response.getTookInMillis());
        for(SearchHit hit: response.getHits()){
//            hit.getFields().get()
            System.out.println(hit.explanation());
            for(String key:hit.getSource().keySet()){
//                System.out.println(" key : " + key+" ,value: "+hit.getSource().get(key));
            }
        }
        client.close();
    }
}

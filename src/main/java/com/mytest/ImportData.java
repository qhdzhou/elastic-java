package com.mytest;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.*;

/**
 * Created by yarui.zhou on 2014/6/13.
 */
public class ImportData {
    public static void main(String[] args) throws Exception {
        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("10.19.2.182", 9300))
                .addTransportAddress(new InetSocketTransportAddress("10.19.2.181", 9300));
        int threadNum = 10;
        if(args != null && args.length >= 1){
            try{
                threadNum = Integer.parseInt(args[0]);
            }catch (Exception e){

            }
        }
//        indexWithBulkProcessor("/home/xiaolei.ji/finalout.txt", client,threadNum);
        indexWithBulkProcessor("fin", client,threadNum);

    }

    public static void indexWithBulkProcessor(String fileName, Client client,int threadNum) throws Exception {
        BufferedReader reader = null;
        long start = System.currentTimeMillis();
        try {
//            读取包内文件
            InputStream is = ImportData.class.getClassLoader().getResourceAsStream(fileName);
            if(is == null){
                throw  new Exception("file do not exist.");
            }
            reader = new BufferedReader(new InputStreamReader(is,"UTF-8"));
//            File file = new File(fileName);
//            if(!file.exists()){
//                throw  new Exception("file do not exist.");
//            }
//            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            BulkProcessor bulkProcessor = buildBulkProcessor(client, threadNum);

            while ((tempString = reader.readLine()) != null) {
                String[] content = tempString.replaceAll("\"","").split(",");
                String poiId = "",poiName = "",poiTypeId = "",poiTypeName = "";
                if(content.length >= 2){
                    poiId = content[0];
                    poiName = content[1];
                }
                if(content.length >=3){
                    poiTypeId = content[2];
                }
                if(content.length >=4){
                    poiTypeName= content[3];
                }
                IndexRequestBuilder response = client.prepareIndex("test", "test")
                        .setSource(XContentFactory.jsonBuilder()
                                        .startObject()
                                        .field("poiId", poiId)
                                        .field("poiName", poiName)
                                        .field("poiTypeId", poiTypeId)
                                        .field("poiTypeName", poiTypeName)
                                        .endObject()
                        );
                bulkProcessor.add(response.request());
                // 显示行号
//                System.out.println("line " + line + ": " + tempString);
                line++;
                if(line % 10000 == 0){
                    long now = System.currentTimeMillis();
                    System.out.println("line " + line + "  completed. It takes " + (now -start)/1000 +"s");
                }
            }
            long now = System.currentTimeMillis();
            System.out.println("completed. It takes " + (now -start)/1000 +"s");
            bulkProcessor.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    private static BulkProcessor buildBulkProcessor(Client client, int threadNum) {
        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("Going to execute new bulk composed of {} actions " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("Executed bulk composed of {} actions " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("Error executing bulk " + failure);
            }
        }).setBulkActions(5*1024*1024).setConcurrentRequests(threadNum).build();
    }

    public static void indexWithSingle(String fileName, Client client) {
        BufferedReader reader = null;
        try {
            InputStream is = ImportData.class.getClassLoader().getResourceAsStream(fileName);
            reader = new BufferedReader(new InputStreamReader(is));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                String[] content = tempString.replaceAll("\"","").split(",");
                String poiId = "",poiName = "",poiTypeId = "",poiTypeName = "";
                if(content.length >= 2){
                    poiId = content[0];
                    poiName = content[1];
                }
                if(content.length >=3){
                    poiTypeId = content[2];
                }
                if(content.length >=4){
                    poiTypeName= content[3];
                }
                IndexResponse response = client.prepareIndex("aospoi", "poi")
                        .setSource(XContentFactory.jsonBuilder()
                                        .startObject()
                                        .field("poiId", poiId)
                                        .field("poiName", poiName)
                                        .field("poiTypeId", poiTypeId)
                                        .field("poiTypeName", poiTypeName)
                                        .endObject()
                        )
                        .execute()
                        .actionGet();
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}

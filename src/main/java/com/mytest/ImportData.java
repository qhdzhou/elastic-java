package com.mytest;

import org.apache.commons.lang.StringUtils;
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
        indexWithBulkProcessor("finalout.txt", client,threadNum);

    }

    public static void indexWithBulkProcessor(String fileName, Client client,int threadNum) throws Exception {
        BufferedReader reader = null;
        long start = System.currentTimeMillis();
        try {
            reader = getBufferedReaderWithResource(fileName);
//            reader =  getBufferedReaderWithFilePath(fileName);
            String tempString ;
            // 一次读入一行，直到读入null为文件结束
            BulkProcessor bulkProcessor = buildBulkProcessor(client, threadNum);
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                String[] content = tempString.replaceAll("\"","").split(",");
                if(content.length == 4){
                    String poiId = content[0];
                    String poiName = content[1];
                    String poiTypeId = content[2];
                    String poiTypeName= content[3];
                    if(StringUtils.isNotEmpty(poiId) && StringUtils.isNotEmpty(poiName)){
                        IndexRequestBuilder response = client.prepareIndex("testindex", "type")
                                .setSource(XContentFactory.jsonBuilder()
                                                .startObject()
                                                .field("poiId", poiId)
                                                .field("poiName", poiName)
                                                .field("poiTypeId", poiTypeId)
                                                .field("poiTypeName", poiTypeName)
                                                .endObject()
                                );
                        bulkProcessor.add(response.request());
                    }
                }
                line++;
                if(line % 10000 == 0){
                    long now = System.currentTimeMillis();
                    System.out.println("line " + line + "  completed. It takes " + (now -start)/1000 +"s");
                }
            }
            long now = System.currentTimeMillis();
            System.out.println("================Completed. It takes " + (now -start)/1000 +"s");
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

    private static BufferedReader getBufferedReaderWithResource(String fileName) throws Exception {
        //            读取包内文件
        InputStream is = ImportData.class.getClassLoader().getResourceAsStream(fileName);
        if(is == null){
            throw  new Exception("file do not exist.");
        }
        return new BufferedReader(new InputStreamReader(is,"UTF-8"));
    }

    private static BufferedReader getBufferedReaderWithFilePath(String fileName) throws Exception {
        File file = new File(fileName);
        if(!file.exists()){
            throw  new Exception("file do not exist.");
        }
        return new BufferedReader(new FileReader(file));
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
        long start = System.currentTimeMillis();
        try {
            InputStream is = ImportData.class.getClassLoader().getResourceAsStream(fileName);
            reader = new BufferedReader(new InputStreamReader(is));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                String[] content = tempString.replaceAll("\"","").split(",");
                if(content.length == 4){
                    String poiId = content[0];
                    String poiName = content[1];
                    String poiTypeId = content[2];
                    String poiTypeName= content[3];
                    if(StringUtils.isNotEmpty(poiId) && StringUtils.isNotEmpty(poiName)){
                        client.prepareIndex("test", "test")
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
                    }
                }
                line++;
                if(line % 10000 == 0){
                    long now = System.currentTimeMillis();
                    System.out.println("line " + line + "  completed. It takes " + (now -start)/1000 +"s");
                }
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

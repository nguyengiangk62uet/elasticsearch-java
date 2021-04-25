package service;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Document;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Configuration
public class DocumentService {
    @Autowired
    private static ObjectMapper objectMapper;

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("192.168.5.104", 9200, "http")));

    /**
     * Ham doc du lieu file tu folder
     * @param folder
     */
    public static void listFilesForFolder(final File folder) {
        System.out.println("Starting read file from folder ...");
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                System.out.println(fileEntry.getName());
            }
        }
    }

    /**
     * Lay toan bo document tu elasticsearch
     * @return
     */
    @Bean
    public static List<Document> getAllDocument() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("bigdata");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        List<Document> documentObjectList = new ArrayList<>();
        SearchResponse searchResponse = null;
        try {
            searchResponse =client.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                SearchHit[] searchHit = searchResponse.getHits().getHits();
                for (SearchHit hit : searchHit) {
                    Map<String, Object> map = hit.getSourceAsMap();
                    documentObjectList.add(objectMapper.convertValue(map, Document.class));
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return documentObjectList;
    }


}

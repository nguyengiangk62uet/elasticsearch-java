package controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Document;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import service.DocumentService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.parser.txt.TXTParser;
import org.xml.sax.SAXException;

@RestController
public class DocumentController {

    @Autowired
    private ObjectMapper objectMapper;

    private RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("192.168.5.104", 9200, "http")));

    /**
     * Return all documents from ES indices
     * @return
     */
    @GetMapping(value = "/getAll", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<Document> getAllDocument() {
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

    /**
     * Indexing documents into ES from files
     * @param folder
     */
    public void indexIntoElasticsearch(final File folder) {
        RestHighLevelClient clientIndex = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.5.104", 9200, "http")));
        // Declare id of document
        long id = 0;
        System.out.println("Starting read file from folder ...");
        long start = System.currentTimeMillis();
        for (final File fileEntry : folder.listFiles()) {
            id++;
            if (fileEntry.isDirectory()) {
                indexIntoElasticsearch(fileEntry);
            } else {
                Map<String, Object> documentMap = new HashMap<>();
                documentMap.put("_1", fileEntry.getName());
                //Detecting the file type
                BodyContentHandler handler = new BodyContentHandler(-1);
                Metadata metadata = new Metadata();
                FileInputStream inputStream = null;
                try {
                    inputStream = new FileInputStream(fileEntry);
                    ParseContext parseContext = new ParseContext();
                    //Text document parser
                    TXTParser  TexTParser = new TXTParser();
                    TexTParser.parse(inputStream, handler, metadata, parseContext);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                documentMap.put("_2", handler.toString());
                // Create index request into ES
                IndexRequest indexRequest = new IndexRequest("post").id(String.valueOf(id)).source(documentMap);
                // Indexing into ES with index response
                try {
                    IndexResponse response = clientIndex.index(indexRequest, RequestOptions.DEFAULT);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        System.out.println("Index complete! " + elapsedTime/1000 + "s");
    }
}

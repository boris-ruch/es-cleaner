package com.boo.escleaner;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class IndicesCleaner {

  private static final String FILEBEAT_VERSION = "filebeat-6.5.1-";

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy.MM");

  @Value("${elasticserchEndpoint}")
  private String elasticsearchEndpoint;

  @Value("#{'${indicesToDelete}'.split(',')}")
  private List<String> indicesToDelete;

  private RestHighLevelClient client;

  @PostConstruct
  public void deleteIndices() {
    client = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticsearchEndpoint, 443, "https")));
    String lastMonth = FORMATTER.format(LocalDate.now().minusMonths(1));
    String indexToDelete = FILEBEAT_VERSION + lastMonth + "*";
    delete(indexToDelete);
    delete(indicesToDelete.stream());
  }

  private void delete(Stream<String> indices) {
    indices.forEach(this::delete);
  }

  private void delete(String indices) {
    log.info("index to delete: '{}'", indices);
    try {
      AcknowledgedResponse response = client.indices().delete(new DeleteIndexRequest(indices), RequestOptions.DEFAULT);
      log.info("response acknowledged :'{}'" + response.isAcknowledged());
    } catch (Exception e) {
      log.error("failed to delete indices '{}'", indices, e);
    }
  }
}

package com.boo.escleaner;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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

  @PostConstruct
  public void deleteIndices() {
    String lastMonth = FORMATTER.format(LocalDate.now().minusMonths(1));
    RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(new HttpHost(elasticsearchEndpoint, 443, "https")));
    String indexToDelete = FILEBEAT_VERSION + lastMonth + "*";
    log.info("index to delete: '{}'", indexToDelete);
    try {
      AcknowledgedResponse response = client.indices()
          .delete(new DeleteIndexRequest(indexToDelete), RequestOptions.DEFAULT);
      log.info("response acknowledged :'{}'" + response.isAcknowledged());
    } catch (Exception e) {
      log.error("failed to delete indices '{}'", indexToDelete, e);
    }
  }
}

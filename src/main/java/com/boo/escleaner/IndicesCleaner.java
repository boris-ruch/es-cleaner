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
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class IndicesCleaner {

  private static final String FILEBEAT_VERSION = "filebeat-6.5.1-";

  private static final DateTimeFormatter FOMATTER = DateTimeFormatter.ofPattern("yyyy.MM");

  @Value("elasticserchEndpoint")
  private String elasticsearchEndpoint;

  @PostConstruct
  public void deleteIndices() {
    String lastMonth = FOMATTER.format(LocalDate.now().minusMonths(1));
    RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(new HttpHost(elasticsearchEndpoint, 443, "https")));
    String indexToDelete = FILEBEAT_VERSION + lastMonth + "*";
    log.info("index to delete: '{}'", indexToDelete);
    DeleteIndexRequest request = new DeleteIndexRequest(indexToDelete);
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");
    try {
      AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
      log.info("response acknoledged :'{}'" + response.isAcknowledged());
    } catch (Exception e) {
      log.error("failed to  delete indices '{}'", indexToDelete, e);
    }
  }
}

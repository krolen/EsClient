package my.es.job;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Created by kkulagin on 2/4/2016.
 */
public class ManageIndexesJob {
  private static final Logger LOG = LoggerFactory.getLogger(ManageIndexesJob.class);
  public static final String ODD_INDEX_ALIAS = "odd";
  public static final String EVEN_INDEX_ALIAS = "even";
  public static final String INDEX_TYPE = "tweet";

  private String serverIp = "10.11.18.53";
  private int serverPort = 9300;

  public ManageIndexesJob() {
  }

  public ManageIndexesJob(String serverIp) {
    this.serverIp = serverIp;
  }

  private Client connect() throws UnknownHostException {
    return TransportClient.builder().build().addTransportAddress(
        new InetSocketTransportAddress(InetAddress.getByName(serverIp), serverPort));
  }

  public void manageIndexes() throws UnknownHostException, ExecutionException, InterruptedException {
    LocalDateTime now = LocalDateTime.now(Clock.systemUTC());
    LocalDateTime hourTime = now.truncatedTo(ChronoUnit.HOURS);
    int hour = hourTime.getHour();
    boolean even = hour % 2 == 0;
    Client client = null;
    try {
      client = connect();
      IndicesAdminClient indicesAdminClient = client.admin().indices();
      GetIndexResponse response = indicesAdminClient.prepareGetIndex().get();
      String[] indices = response.getIndices();
      // delete old ones
      Arrays.stream(indices).filter((s) -> s.startsWith("my_")).
          mapToInt((s) -> Integer.valueOf(s.substring("my_".length()))).
          forEachOrdered((i) -> {
            if (i < hour - 1) {
              LOG.info("Removing index my_" + i);
              DeleteIndexResponse indexResponse = indicesAdminClient.delete(Requests.deleteIndexRequest("my_" + String.valueOf(i))).actionGet();
              if(indexResponse.isAcknowledged()) {
                LOG.info("Removed my_" + i);
              } else {
                LOG.error("Cannot remove index " + indexResponse.toString());
              }
            }
          });
      ensureIndexExists(indicesAdminClient, "my_" + String.valueOf(hour - 1), even ? ODD_INDEX_ALIAS : EVEN_INDEX_ALIAS);
      ensureIndexExists(indicesAdminClient, "my_" + String.valueOf(hour), even ? EVEN_INDEX_ALIAS : ODD_INDEX_ALIAS);
    } finally {
      Optional.ofNullable(client).ifPresent(Client::close);
    }
  }

  private boolean ensureIndexExists(IndicesAdminClient indicesAdminClient, String indexName, String alias) throws ExecutionException, InterruptedException {
    try {
      indicesAdminClient.prepareGetIndex().setIndices(indexName).get();
      return false;
    } catch (IndexNotFoundException e) {
      LOG.info("Adding index " + indexName + " with alias " + alias);
      Settings settings = Settings.builder().
          put("number_of_shards", 1).
          put("number_of_replicas", 0).
          put("refresh_interval", "10s").
          put("index.store.type", "mmapfs").
          build();
      CreateIndexRequest createIndexRequest = Requests.createIndexRequest(indexName).alias(new Alias(alias)).settings(settings);
      indicesAdminClient.create(createIndexRequest);
      LOG.info("Added index " + indexName + " with alias " + alias);
      return true;
    }
  }


  public static void main(String[] args) throws InterruptedException, ExecutionException, UnknownHostException {
    ManageIndexesJob job = new ManageIndexesJob();
    if(args.length > 0) {
      job.setServerIp(args[0]);
    }
    if(args.length > 1) {
      job.setServerPort(Integer.parseInt(args[1]));
    }
    job.manageIndexes();
  }

  public void setServerIp(String serverIp) {
    this.serverIp = serverIp;
  }

  public void setServerPort(int serverPort) {
    this.serverPort = serverPort;
  }
}

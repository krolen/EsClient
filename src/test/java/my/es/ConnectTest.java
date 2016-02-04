package my.es;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by kkulagin on 2/1/2016.0
 */
public class ConnectTest {

  public static final String INDEX_NAME = "tweets";
  public static final String INDEX_ALIAS = "tweets_alias";
  public static final String DATA_TYPE = "tweet";

  private Client client;

  @Before
  public void setUp() throws Exception {

    client = TransportClient.builder().build().addTransportAddress(
        new InetSocketTransportAddress(InetAddress.getByName("10.11.18.53"), 9300));
    IndicesAdminClient indicesAdminClient = client.admin().indices();

    GetIndexResponse response = indicesAdminClient.getIndex(new GetIndexRequest().indices(INDEX_NAME)).get();
//    DeleteIndexRequest tweets = Requests.deleteIndexRequest(INDEX_NAME);
//    indicesAdminClient.delete(tweets);
//    DeleteIndexRequest tweets = Requests.deleteIndexRequest(INDEX_NAME + 2);
//    indices.delete(tweets);

    Settings settings = Settings.builder().
        put("number_of_shards", 1).
        put("number_of_replicas", 0).
        put("refresh_interval", "5s").
        build();
    CreateIndexRequest createIndexRequest = Requests.createIndexRequest(INDEX_NAME).alias(new Alias(INDEX_ALIAS)).
        settings(settings);
    indicesAdminClient.create(createIndexRequest);
//    createIndexRequest = Requests.createIndexRequest(INDEX_NAME + 2).settings(settings);
//    indices.create(createIndexRequest);

  }

  @Test
  public void testGet() {
    QueryBuilder qb = QueryBuilders.queryStringQuery("content:*yo5*");
//    QueryBuilder qb = termQuery("content", "*yo5*");

    SearchResponse scrollResp = client.prepareSearch(INDEX_ALIAS)
        .setScroll(new TimeValue(60000))
        .setQuery(qb)
        .setSize(1000).execute().actionGet(); //100 hits per shard will be returned for each scroll
//Scroll until no hits are returned
    while (true) {

      SearchHit[] hits = scrollResp.getHits().getHits();
      System.out.println(hits.length);
      for (SearchHit hit : hits) {
        //Handle the hit...
      }
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
      //Break condition: No hits are returned
      if (hits.length == 0) {
        break;
      }
    }
//    Requests.searchScrollRequest()
//    SearchScrollRequest scrollRequest = new SearchScrollRequest();
//    scrollRequest.
//    client.searchScroll(
//        scrollRequest
//    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
//    bulkRequestBuilder.get()

  }

  @Test
  public void testAdd() throws IOException {
    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
    long start = System.currentTimeMillis();
    LongStream.range(0, 20000).forEach((i) -> {
      Tweet tweet = new Tweet(i, "yoyoyo" + i);
      try {
        bulkRequestBuilder.add(
            client.prepareIndex(INDEX_ALIAS, DATA_TYPE).
                setId(String.valueOf(tweet.getId())).
                setSource(jsonBuilder().
                    startObject().
                    field("content", tweet.getContents()).
                    endObject()));
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    BulkResponse responses = bulkRequestBuilder.get();
    long end = System.currentTimeMillis();
    if(responses.hasFailures()) {
      System.out.println(responses.buildFailureMessage());
    }
    System.out.println(end - start);
  }

  @Test
  public void testTransportConnect() throws UnknownHostException {

  }

  @After
  public void tearDown() throws Exception {
    client.close();
  }
}

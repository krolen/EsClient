package my.es;

/**
 * Created by kkulagin on 2/2/2016.
 */
public class Tweet {
  private long id;
  private String contents;

  public Tweet() {
  }

  public Tweet(long id, String contents) {
    this.id = id;
    this.contents = contents;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getContents() {
    return contents;
  }

  public void setContents(String contents) {
    this.contents = contents;
  }
}

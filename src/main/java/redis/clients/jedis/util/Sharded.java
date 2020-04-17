package redis.clients.jedis.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 基于客户端分片的实现
 * @param <R>
 * @param <S>
 */
public class Sharded<R, S extends ShardInfo<R>> {

  public static final int DEFAULT_WEIGHT = 1;
  private TreeMap<Long, S> nodes;
  private final Hashing algo;
  private final Map<ShardInfo<R>, R> resources = new LinkedHashMap<ShardInfo<R>, R>();

  /**
   * The default pattern used for extracting a key tag. The pattern must have a group (between
   * parenthesis), which delimits the tag to be hashed. A null pattern avoids applying the regular
   * expression for each lookup, improving performance a little bit is key tags aren't being used.
   */
  private Pattern tagPattern = null;
  // the tag is anything between {}
  public static final Pattern DEFAULT_KEY_TAG_PATTERN = Pattern.compile("\\{(.+?)\\}");

  public Sharded(List<S> shards) {
    this(shards, Hashing.MURMUR_HASH); // MD5 is really not good as we works
    // with 64-bits not 128
  }

  public Sharded(List<S> shards, Hashing algo) {
    this.algo = algo;
    initialize(shards);
  }

  public Sharded(List<S> shards, Pattern tagPattern) {
    this(shards, Hashing.MURMUR_HASH, tagPattern); // MD5 is really not good
    // as we works with
    // 64-bits not 128
  }

  /**
   * shards，可以指定redis的服务信息对象的list集合（ShardInfo的子类，如JedisShardInfo，存放了redis子节点的ip、端口、weight等信息）。
   * hash算法（默认一致性hash），jedis中指定了两种hash实现，一种是一致性hash，一种是基于md5的实现，在redis.clients.util.Hashing中指定的。
   * tagPattern，可以指定按照key的某一部分进行hash分片（比如我们可以将以order开头的key分配到redis节点1上，可以将以product开头的key分配到redis节点2上），默认情况下是根据整个key进行hash分片的。
   *
   * @param shards     * shards，可以指定redis的服务信息对象的list集合（ShardInfo的子类，
   *                   如JedisShardInfo，存放了redis子节点的ip、端口、weight等信息）。
   * @param algo    * hash算法（默认一致性hash），jedis中指定了两种hash实现，一种是一致性hash，
   *                一种是基于md5的实现，在redis.clients.util.Hashing中指定的。
   * @param tagPattern    * 可以指定按照key的某一部分进行hash分片（比如我们可以将以order开头的key分配到redis节点1上，
   *                      可以将以product开头的key分配到redis节点2上），默认情况下是根据整个key进行hash分片的。
   */
  public Sharded(List<S> shards, Hashing algo, Pattern tagPattern) {
    this.algo = algo;
    this.tagPattern = tagPattern;
    initialize(shards);
  }

  /**
   * 1、首先根据redis节点集合信息创建虚拟节点（一致性hash上0~2^32之间的点），通过下面的源码可以看出，
   * 根据每个redis节点的name计算出对应的hash值（如果没有配置节点名称，就是用默认的名字），
   * 并创建了160*weight个虚拟节点，weight默认情况下等于1，如果某个节点的配置较高，可以适当的提高虚拟节点
   * 的个数，将更多的请求打到这个节点上。
   *
   * 2、Sharded中使用TreeMap来实现hash环。
   *
   * @param shards
   */
  private void initialize(List<S> shards) {
    nodes = new TreeMap<Long, S>();

    for (int i = 0; i != shards.size(); ++i) {
      final S shardInfo = shards.get(i);
      if (shardInfo.getName() == null) for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
        nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), shardInfo);
      }
      else for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
        nodes.put(this.algo.hash(shardInfo.getName() + "*" + n), shardInfo);
      }
      resources.put(shardInfo, shardInfo.createResource());
    }
  }

  public R getShard(byte[] key) {
    return resources.get(getShardInfo(key));
  }

  public R getShard(String key) {
    return resources.get(getShardInfo(key));
  }

  /**
   * //首先通过key或keytag计算出hash值，然后在TreeMap中找到比这个hash值大的第一个虚拟节点
   * （这个过程就是在一致性hash环上顺时针查找的过程），如果这个hash值大于所有虚拟节点对应的hash，
   * 则使用第一个虚拟节点
   * @param key
   * @return
   */
  public S getShardInfo(byte[] key) {
    SortedMap<Long, S> tail = nodes.tailMap(algo.hash(key));
    if (tail.isEmpty()) {//表明新的key的hash值最大
      return nodes.get(nodes.firstKey());
    }
    return tail.get(tail.firstKey());
  }

  //注意getKeyTag()方法
  public S getShardInfo(String key) {
    return getShardInfo(SafeEncoder.encode(getKeyTag(key)));
  }

  /**
   * A key tag is a special pattern inside a key that, if preset, is the only part of the key hashed
   * in order to select the server for this key.
   * @see <a href="http://redis.io/topics/partitioning">partitioning</a>
   * @param key
   * @return The tag if it exists, or the original key
   */
  /*
  * //在介绍Sharded的构造方法时，指定了一个tagPattern，它的作用就是在使用key进行分片操作时，
  * 可以根据key的一部分来计算分片，getKeyTag（）方法用来获取key对应的keytag，默认情况下是根据整个key来分片,
   * */
  public String getKeyTag(String key) {
    if (tagPattern != null) {
      Matcher m = tagPattern.matcher(key);
      if (m.find()) return m.group(1);
    }
    return key;
  }

  public Collection<S> getAllShardInfo() {
    return Collections.unmodifiableCollection(nodes.values());
  }

  public Collection<R> getAllShards() {
    return Collections.unmodifiableCollection(resources.values());
  }
}

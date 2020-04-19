package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterMaxAttemptsException;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.jedis.util.JedisClusterCRC16;

/**
 **该类主要由两个重点：采用模板方法设计，具体存取操作有子类实现
 **在集群操作中，为了保证高可用方式，采用递归算法进行尝试发生的MOVED,ASK,数据迁移操作等。
 **/
public abstract class JedisClusterCommand<T> {
  // JedisCluster的连接真正持有类
  private final JedisClusterConnectionHandler connectionHandler;
  // 尝试次数，默认为5
  private final int maxAttempts;

  public JedisClusterCommand(JedisClusterConnectionHandler connectionHandler, int maxAttempts) {
    this.connectionHandler = connectionHandler;
    this.maxAttempts = maxAttempts;
  }
  // redis各种操作的抽象方法，JedisCluster中都是匿名内部类实现。
  public abstract T execute(Jedis connection);

  public T run(String key) {
    return runWithRetries(JedisClusterCRC16.getSlot(key), this.maxAttempts, false, null);
  }

  public T run(int keyCount, String... keys) {
    if (keys == null || keys.length == 0) {
      throw new JedisClusterOperationException("No way to dispatch this command to Redis Cluster.");
    }

    // For multiple keys, only execute if they all share the same connection slot.
    int slot = JedisClusterCRC16.getSlot(keys[0]);
    if (keys.length > 1) {
      for (int i = 1; i < keyCount; i++) {
        int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
        if (slot != nextSlot) {
          throw new JedisClusterOperationException("No way to dispatch this command to Redis "
              + "Cluster because keys have different slots.");
        }
      }
    }

    return runWithRetries(slot, this.maxAttempts, false, null);
  }

  public T runBinary(byte[] key) {
    return runWithRetries(JedisClusterCRC16.getSlot(key), this.maxAttempts, false, null);
  }

  public T runBinary(int keyCount, byte[]... keys) {
    if (keys == null || keys.length == 0) {
      throw new JedisClusterOperationException("No way to dispatch this command to Redis Cluster.");
    }

    // For multiple keys, only execute if they all share the same connection slot.
    int slot = JedisClusterCRC16.getSlot(keys[0]);
    if (keys.length > 1) {
      for (int i = 1; i < keyCount; i++) {
        int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
        if (slot != nextSlot) {
          throw new JedisClusterOperationException("No way to dispatch this command to Redis "
              + "Cluster because keys have different slots.");
        }
      }
    }

    return runWithRetries(slot, this.maxAttempts, false, null);
  }

  public T runWithAnyNode() {
    Jedis connection = null;
    try {
      connection = connectionHandler.getConnection();
      return execute(connection);
    } catch (JedisConnectionException e) {
      throw e;
    } finally {
      releaseConnection(connection);
    }
  }

  /***
   *** 该方法采用递归方式，保证在往集群中存取数据时，发生MOVED,ASKing,数据迁移过程中遇到问题，也是一种实现高可用的方式。
   ***该方法中调用execute方法，该方法由子类具体实现。
   ***/
  /**
   * 利用重试机制运行键命令
   *
   * @param slot
   *            要操作的槽
   * @param attempts
   *            重试次数，每重试一次减1
   * @param tryRandomNode
   *            标识是否随机获取活跃节点连接，true为是，false为否
   * @param redirect
   * @return
   */

  private T runWithRetries(final int slot, int attempts, boolean tryRandomNode, JedisRedirectionException redirect) {
    if (attempts <= 0) {
      throw new JedisClusterMaxAttemptsException("No more cluster attempts left.");
    }

    Jedis connection = null;
    try {
      /**
       *第一执行该方法，null。只有发生JedisAskDataException
       *异常时，才redirect才有值
       **/
      if (redirect != null) {
        connection = this.connectionHandler.getConnectionFromNode(redirect.getTargetNode());
        if (redirect instanceof JedisAskDataException) {
          // TODO: Pipeline asking with the original command to make it faster....
          connection.asking();
        }
      } else {
        // 第一次执行时，tryRandomNode为false。
        if (tryRandomNode) {
          connection = connectionHandler.getConnection();
        } else {
//          根据slot获取分配的槽数，然后根据数据槽从JedisClusterInfoCache 中获取Jedis的实例
          connection = connectionHandler.getConnectionFromSlot(slot);
        }
      }
//      调用子类方法的具体实现
      return execute(connection);

    } catch (JedisNoReachableClusterNodeException jnrcne) {
      throw jnrcne;
    } catch (JedisConnectionException jce) {
      // release current connection before recursion
      //释放已有的连接
      releaseConnection(connection);
      connection = null;
      /***
       ***只是重建键值对slot-jedis缓存即可。已经没有剩余的redirection了。
       ***已经达到最大的MaxRedirection次数，抛出异常即可。
       ***/
      if (attempts <= 1) {
        //We need this because if node is not reachable anymore - we need to finally initiate slots
        //renewing, or we can stuck with cluster state without one node in opposite case.
        //But now if maxAttempts = [1 or 2] we will do it too often.
        //TODO make tracking of successful/unsuccessful operations for node - do renewing only
        //if there were no successful responses from this node last few seconds
        this.connectionHandler.renewSlotCache();
      }
      // 递归调用该方法
      return runWithRetries(slot, attempts - 1, tryRandomNode, redirect);
    } catch (JedisRedirectionException jre) {
      // if MOVED redirection occurred,
      // 发生MovedException,需要重建键值对slot-Jedis的缓存。
      if (jre instanceof JedisMovedDataException) {
        // it rebuilds cluster's slot cache recommended by Redis cluster specification
        this.connectionHandler.renewSlotCache(connection);
      }

      // release current connection before recursion
      releaseConnection(connection);
      connection = null;
      // 递归调用。
      return runWithRetries(slot, attempts - 1, false, jre);
    } finally {
      releaseConnection(connection);
    }
  }

  private void releaseConnection(Jedis connection) {
    if (connection != null) {
      connection.close();
    }
  }

}

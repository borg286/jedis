package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.SafeEncoder;

public class PublishSubscribeCommandsTest extends JedisCommandTestBase {
  private void publishOne(final String channel, final String message) {
    Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          Jedis j = createJedis();
          j.publish(channel, message);
          j.disconnect();
        } catch (Exception ex) {
        }
      }
    });
    t.start();
  }

  @Test
  public void subscribe() throws InterruptedException {
    jedis.subscribe(new JedisPubSub() {
      public void onMessage(String channel, String message) {
        assertEquals("foo", channel);
        assertEquals("exit", message);
        unsubscribe();
      }

      public void onSubscribe(String channel, int subscribedChannels) {
        assertEquals("foo", channel);
        assertEquals(1, subscribedChannels);

        // now that I'm subscribed... publish
        publishOne("foo", "exit");
      }

      public void onUnsubscribe(String channel, int subscribedChannels) {
        assertEquals("foo", channel);
        assertEquals(0, subscribedChannels);
      }
    }, "foo");
  }

  @Test
  public void pubSubChannels() {
    final List<String> expectedActiveChannels = Arrays
        .asList("testchan1", "testchan2", "testchan3");
    jedis.subscribe(new JedisPubSub() {
      private int count = 0;

      @Override
      public void onSubscribe(String channel, int subscribedChannels) {
        count++;
        // All channels are subscribed
        if (count == 3) {
          Jedis otherJedis = createJedis();
          List<String> activeChannels = otherJedis.pubsubChannels("test*");
          assertTrue(expectedActiveChannels.containsAll(activeChannels));
          unsubscribe();
        }
      }
    }, "testchan1", "testchan2", "testchan3");
  }

  @Test
  public void pubSubChannelWithPingPong() throws InterruptedException {
    final CountDownLatch latchUnsubscribed = new CountDownLatch(1);
    final CountDownLatch latchReceivedPong = new CountDownLatch(1);
    jedis.subscribe(new JedisPubSub() {

      @Override
      public void onSubscribe(String channel, int subscribedChannels) {
        publishOne("testchan1", "hello");
      }

      @Override
      public void onMessage(String channel, String message) {
        this.ping();
      }

      @Override
      public void onPong(String pattern) {
        latchReceivedPong.countDown();
        unsubscribe();
      }

      @Override
      public void onUnsubscribe(String channel, int subscribedChannels) {
        latchUnsubscribed.countDown();
      }
    }, "testchan1");
    assertEquals(0L, latchReceivedPong.getCount());
    assertEquals(0L, latchUnsubscribed.getCount());
  }

  @Test
  public void pubSubNumPat() {
    jedis.psubscribe(new JedisPubSub() {
      private int count = 0;

      @Override
      public void onPSubscribe(String pattern, int subscribedChannels) {
        count++;
        if (count == 3) {
          Jedis otherJedis = createJedis();
          Long numPatterns = otherJedis.pubsubNumPat();
          assertEquals(new Long(2l), numPatterns);
          punsubscribe();
        }
      }

    }, "test*", "test*", "chan*");
  }

  @Test
  public void pubSubNumSub() {
    final Map<String, String> expectedNumSub = new HashMap<String, String>();
    expectedNumSub.put("testchannel2", "1");
    expectedNumSub.put("testchannel1", "1");
    jedis.subscribe(new JedisPubSub() {
      private int count = 0;

      @Override
      public void onSubscribe(String channel, int subscribedChannels) {
        count++;
        if (count == 2) {
          Jedis otherJedis = createJedis();
          Map<String, String> numSub = otherJedis.pubsubNumSub("testchannel1", "testchannel2");
          assertEquals(expectedNumSub, numSub);
          unsubscribe();
        }
      }
    }, "testchannel1", "testchannel2");
  }

  @Test
  public void subscribeMany() throws UnknownHostException, IOException, InterruptedException {
    jedis.subscribe(new JedisPubSub() {
      public void onMessage(String channel, String message) {
        unsubscribe(channel);
      }

      public void onSubscribe(String channel, int subscribedChannels) {
        publishOne(channel, "exit");
      }

    }, "foo", "bar");
  }

  @Test
  public void psubscribe() throws UnknownHostException, IOException, InterruptedException {
    jedis.psubscribe(new JedisPubSub() {
      public void onPSubscribe(String pattern, int subscribedChannels) {
        assertEquals("foo.*", pattern);
        assertEquals(1, subscribedChannels);
        publishOne("foo.bar", "exit");

      }

      public void onPUnsubscribe(String pattern, int subscribedChannels) {
        assertEquals("foo.*", pattern);
        assertEquals(0, subscribedChannels);
      }

      public void onPMessage(String pattern, String channel, String message) {
        assertEquals("foo.*", pattern);
        assertEquals("foo.bar", channel);
        assertEquals("exit", message);
        punsubscribe();
      }
    }, "foo.*");
  }

  @Test
  public void psubscribeMany() throws UnknownHostException, IOException, InterruptedException {
    jedis.psubscribe(new JedisPubSub() {
      public void onPSubscribe(String pattern, int subscribedChannels) {
        publishOne(pattern.replace("*", "123"), "exit");
      }

      public void onPMessage(String pattern, String channel, String message) {
        punsubscribe(pattern);
      }
    }, "foo.*", "bar.*");
  }

  @Test
  public void subscribeLazily() throws UnknownHostException, IOException, InterruptedException {
    final JedisPubSub pubsub = new JedisPubSub() {
      public void onMessage(String channel, String message) {
        unsubscribe(channel);
      }

      public void onSubscribe(String channel, int subscribedChannels) {
        publishOne(channel, "exit");
        if (!channel.equals("bar")) {
          this.subscribe("bar");
          this.psubscribe("bar.*");
        }
      }

      public void onPSubscribe(String pattern, int subscribedChannels) {
        publishOne(pattern.replace("*", "123"), "exit");
      }

      public void onPMessage(String pattern, String channel, String message) {
        punsubscribe(pattern);
      }
    };

    jedis.subscribe(pubsub, "foo");
  }

  @Test
  public void binarySubscribe() throws UnknownHostException, IOException, InterruptedException {
    jedis.subscribe(new BinaryJedisPubSub() {
      public void onMessage(byte[] channel, byte[] message) {
        assertTrue(Arrays.equals(SafeEncoder.encode("foo"), channel));
        assertTrue(Arrays.equals(SafeEncoder.encode("exit"), message));
        unsubscribe();
      }

      public void onSubscribe(byte[] channel, int subscribedChannels) {
        assertTrue(Arrays.equals(SafeEncoder.encode("foo"), channel));
        assertEquals(1, subscribedChannels);
        publishOne(SafeEncoder.encode(channel), "exit");
      }

      public void onUnsubscribe(byte[] channel, int subscribedChannels) {
        assertTrue(Arrays.equals(SafeEncoder.encode("foo"), channel));
        assertEquals(0, subscribedChannels);
      }
    }, SafeEncoder.encode("foo"));
  }

  @Test
  public void binarySubscribeMany() throws UnknownHostException, IOException, InterruptedException {
    jedis.subscribe(new BinaryJedisPubSub() {
      public void onMessage(byte[] channel, byte[] message) {
        unsubscribe(channel);
      }

      public void onSubscribe(byte[] channel, int subscribedChannels) {
        publishOne(SafeEncoder.encode(channel), "exit");
      }
    }, SafeEncoder.encode("foo"), SafeEncoder.encode("bar"));
  }

  @Test
  public void binaryPsubscribe() throws UnknownHostException, IOException, InterruptedException {
    jedis.psubscribe(new BinaryJedisPubSub() {
      public void onPSubscribe(byte[] pattern, int subscribedChannels) {
        assertTrue(Arrays.equals(SafeEncoder.encode("foo.*"), pattern));
        assertEquals(1, subscribedChannels);
        publishOne(SafeEncoder.encode(pattern).replace("*", "bar"), "exit");
      }

      public void onPUnsubscribe(byte[] pattern, int subscribedChannels) {
        assertTrue(Arrays.equals(SafeEncoder.encode("foo.*"), pattern));
        assertEquals(0, subscribedChannels);
      }

      public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
        assertTrue(Arrays.equals(SafeEncoder.encode("foo.*"), pattern));
        assertTrue(Arrays.equals(SafeEncoder.encode("foo.bar"), channel));
        assertTrue(Arrays.equals(SafeEncoder.encode("exit"), message));
        punsubscribe();
      }
    }, SafeEncoder.encode("foo.*"));
  }

  @Test
  public void binaryPsubscribeMany() throws UnknownHostException, IOException, InterruptedException {
    jedis.psubscribe(new BinaryJedisPubSub() {
      public void onPSubscribe(byte[] pattern, int subscribedChannels) {
        publishOne(SafeEncoder.encode(pattern).replace("*", "123"), "exit");
      }

      public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
        punsubscribe(pattern);
      }
    }, SafeEncoder.encode("foo.*"), SafeEncoder.encode("bar.*"));
  }

  @Test
  public void binarySubscribeLazily() throws UnknownHostException, IOException,
      InterruptedException {
    final BinaryJedisPubSub pubsub = new BinaryJedisPubSub() {
      public void onMessage(byte[] channel, byte[] message) {
        unsubscribe(channel);
      }

      public void onSubscribe(byte[] channel, int subscribedChannels) {
        publishOne(SafeEncoder.encode(channel), "exit");

        if (!SafeEncoder.encode(channel).equals("bar")) {
          this.subscribe(SafeEncoder.encode("bar"));
          this.psubscribe(SafeEncoder.encode("bar.*"));
        }
      }

      public void onPSubscribe(byte[] pattern, int subscribedChannels) {
        publishOne(SafeEncoder.encode(pattern).replace("*", "123"), "exit");
      }

      public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
        punsubscribe(pattern);
      }
    };

    jedis.subscribe(pubsub, SafeEncoder.encode("foo"));
  }

  @Test(expected = JedisConnectionException.class)
  public void unsubscribeWhenNotSusbscribed() throws InterruptedException {
    JedisPubSub pubsub = new JedisPubSub() {
    };
    pubsub.unsubscribe();
  }


  private String makeLargeString(int size) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < size; i++)
      sb.append((char) ('a' + i % 26));

    return sb.toString();
  }
}

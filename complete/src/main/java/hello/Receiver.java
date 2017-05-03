package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.CountDownLatch;

public class Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private CountDownLatch latch;

    StringRedisTemplate redisTemplate;

    @Autowired
    public Receiver(CountDownLatch latch, StringRedisTemplate redisTemplate) {
        this.latch = latch;
        this.redisTemplate = redisTemplate;
    }

    public void receiveMessage(String message) {

        LOGGER.info("Received <" + message + ">");
        String value = redisTemplate.opsForList().rightPop("list");
        System.out.println("got queue value :"+value);
        latch.countDown();
    }
}

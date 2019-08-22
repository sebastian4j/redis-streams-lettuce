package com.sebastian.redis.streams;

import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.lettuce.core.RedisClient;

/**
 * productor de mensajes en el stream.
 * 
 * https://github.com/lettuce-io/lettuce-core/wiki/Connection-Pooling
 * 
 * https://github.com/lettuce-io/lettuce-core/releases
 * 
 * @author Sebastián Ávila A.
 *
 */
public class StreamProductor {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProductor.class);
  private static final int MENSAJES_ENVIAR = 500_00;

  public static void main(String[] args) throws InterruptedException {
    final var cliente = RedisClient.create(StreamsConstantes.REDIS_URI);
    try (final var con = cliente.connect()) {
      final var cmd = con.sync();
      int i = 0;
      while (i < MENSAJES_ENVIAR) {
        final var res =
            cmd.xadd(StreamsConstantes.NOMBRE_STREAM, Map.of(UUID.randomUUID().toString(), String.valueOf(i)));
        i++;
        LOGGER.info("enviados: {} - id: {}", i, res);
        Thread.sleep(1_000);
      }
    }
    cliente.shutdown();
  }
}

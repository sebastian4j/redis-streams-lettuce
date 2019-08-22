package com.sebastian.redis.streams;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;

/**
 * consume los mensajes del stream.
 * 
 * info:
 * 
 * https://github.com/lettuce-io/lettuce-core/releases
 * 
 * @author Sebastián Ávila A.
 *
 */
public class StreamCliente {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamCliente.class);

  /**
   * el objetivo es leer todos mensajes disponibles.
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) {
    final var cliente = RedisClient.create(StreamsConstantes.REDIS_URI);
    String ultimo = "0";
    boolean leyendo = true;
    var cuentaMensajes = 0;
    try (final var con = cliente.connect()) {
      final var cmd = con.sync();
      while (leyendo) {
        final var mensajes = cmd.xread(XReadArgs.Builder.block(Duration.ofSeconds(5)).count(1),
            StreamOffset.from(StreamsConstantes.NOMBRE_STREAM, ultimo));
        cuentaMensajes += mensajes.size();
        LOGGER.info("leidos: {}", cuentaMensajes);
        for (final var m : mensajes) {
          LOGGER.info("{}", m.getId());
          ultimo = m.getId();
        }
      }
    }
    cliente.shutdown();
  }
}

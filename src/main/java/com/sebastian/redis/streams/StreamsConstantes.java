package com.sebastian.redis.streams;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

/**
 * constantes para las pruebas de streams.
 * 
 * @author Sebastián Ávila A.
 */
public final class StreamsConstantes {
   public static final String NOMBRE_STREAM = "numeros";
   public static final String REDIS_URI = "redis://localhost:6379";
   public static final String REDIS_SERVIDOR = "localhost";
   public static final int REDIS_PUERTO = 6379;
   
   /**
    * crea un cliente para redis.
    * 
    * @param nombre nombre que identificará al cliente
    * @return cliente redis
    */
   public static RedisClient crearCliente(final String nombre) {
     return RedisClient.create(
            RedisURI.Builder.redis(REDIS_SERVIDOR, REDIS_PUERTO).withClientName(nombre)
                    .build());
   }
}

package com.sebastian.redis.streams;

import io.lettuce.core.Consumer;
import io.lettuce.core.ExceptionFactory;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.XReadArgs;
import java.time.Duration;

/**
 * grupo de consumidores de mensajes.
 *
 * @author Sebastián Ávila A.
 */
public class ConsumerGroup {

  private final String nombre;
  private final String grupo;

  private ConsumerGroup(final String nombre, final String grupo) {
    this.nombre = nombre;
    this.grupo = grupo;
  }

  public static void main(String[] args) {
    new Thread(() -> new ConsumerGroup("consumidor1", "grupo1").consumir()).start();
    new Thread(() -> new ConsumerGroup("consumidor2", "grupo1").consumir()).start();
  }

  /**
   * crea el grupo de consumidores.
   */
  private void crear() {
    // Xgroup create numeros group0 0 MKSTREAM
    try {
      final var cliente = StreamsConstantes.crearCliente(nombre);
      try (final var con = cliente.connect()) {
        final var cmd = con.sync();
        System.out.println(cmd.xgroupCreate(XReadArgs.StreamOffset.latest(StreamsConstantes.NOMBRE_STREAM), grupo));
      }
      cliente.shutdown();
    } catch (RedisBusyException e) {
      System.out.println("el nombre ya existe, continuar");
    }
  }

  /**
   * consume mensajes del stream utilizando el grupo y nombre indicado.
   */
  private void consumir() {
    crear();
    System.out.println("consumir mensajes desde el cliente: " + nombre + " y grupo: " + grupo);
    final var cliente = StreamsConstantes.crearCliente(nombre);
    boolean leyendo = true;
    try (final var con = cliente.connect()) {
      while (leyendo) {
        final var cmd = con.sync();
        final var mensajes
                = cmd.xreadgroup(Consumer.from(grupo, nombre), XReadArgs.Builder.block(Duration.ofSeconds(5)).count(1),
                        XReadArgs.StreamOffset.from(StreamsConstantes.NOMBRE_STREAM, ">"));
        System.out.println(nombre + " " + grupo + " mensajes leidos: " + mensajes.size());
        mensajes.forEach(s -> {
          System.out.println("____________________________");
          System.out.print("cliente: " + nombre);
          System.out.print(" id: " + s.getId());
          System.out.print(" stream: " + s.getStream());
          s.getBody().forEach((a, b) -> {
            System.out.println(" " + b);
          });
          cmd.xack(StreamsConstantes.NOMBRE_STREAM, grupo, s.getId());
        });        
      }
    }
    cliente.shutdown();
  }
}

package ma.enset.kafka.handlers;

import ma.enset.kafka.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    /** Consumer KAFKA : lit les PageEvent du topic R1 et les affiche. */
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> {
            System.out.println("**********************");
            System.out.println("Message reçu : " + input);
            System.out.println("**********************");
        };
    }

    /** Supplier KAFKA : appelé par le poller (toutes les 200 ms), publie un PageEvent aléatoire sur R2. */
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                new Random().nextInt(9000));
    }

    /** Function (consumer + producer) : lit R2, transforme l'événement, publie sur R3. */
    @Bean
    public Function<PageEvent, PageEvent> pageEventFunction() {
        return input -> new PageEvent(
                "L:" + input.name().length(),
                "UUUUUU",
                input.date(),
                input.duration());
    }

    /**
     * Kafka Streams : Real Time Stream Processing sur R2.
     * - garde les visites de plus de 100 ms
     * - clé = nom de la page, valeur = durée
     * - regroupement par page, fenêtre de 5 secondes
     * - comptage du nombre de visites -> store "count-store" (interrogeable) + topic R4
     */
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return input -> input
                .filter((k, v) -> v.duration() > 100)
                .map((k, v) -> new KeyValue<>(v.name(), v.duration()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.as("count-store"))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v));
    }
}

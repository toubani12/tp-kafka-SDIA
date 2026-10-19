package ma.enset.kafka.web;

import ma.enset.kafka.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventRestController {

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;

    public PageEventRestController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    /** Producer KAFKA via REST : publie un PageEvent dans le topic indiqué. */
    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name) {
        PageEvent pageEvent = new PageEvent(
                name,
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                new Random().nextInt(9000));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    /**
     * Server-Sent Events : chaque seconde, interroge le store Kafka Streams "count-store"
     * et renvoie le nombre de visites par page sur les 5 dernières secondes.
     */
    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {
                    Map<String, Long> stringLongMap = new HashMap<>();
                    try {
                        ReadOnlyWindowStore<String, Long> windowStore =
                                interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());
                        Instant now = Instant.now();
                        Instant from = now.minusMillis(5000);
                        try (KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now)) {
                            while (fetchAll.hasNext()) {
                                KeyValue<Windowed<String>, Long> next = fetchAll.next();
                                stringLongMap.put(next.key.key(), next.value);
                            }
                        }
                    } catch (Exception e) {
                        // le store n'est pas encore prêt (démarrage / rebalance) : on renvoie un résultat vide
                    }
                    return stringLongMap;
                })
                .share();
    }
}

package ma.enset.kafka.events;

import java.util.Date;

/**
 * Événement "visite d'une page" : quelle page, quel utilisateur, quand et combien de temps (ms).
 */
public record PageEvent(String name, String user, Date date, long duration) {
}

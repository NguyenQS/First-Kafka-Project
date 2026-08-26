# Kafka Interview Questions

> Diese Antworten habe ich während des Lernens selbst entwickelt und anschließend sprachlich überarbeitet.

---

## Was macht flush()?

**Meine Antwort:**

`flush()` sorgt dafür, dass alle aktuell gepufferten Nachrichten an Kafka gesendet werden, bevor das Programm beendet wird.

---

## Warum brauchen wir `--from-beginning`?

**Meine Antwort:**

`--from-beginning` sorgt dafür, dass ein neuer Consumer alle bereits im Topic vorhandenen Nachrichten ab Offset 0 liest. Ohne diese Option würde er normalerweise nur neue Nachrichten lesen, die nach seinem Start eintreffen.

---

## Was bedeutet `auto_offset_reset="earliest"`?

**Meine Antwort:**

Wenn für diese Consumer Group noch kein Offset gespeichert ist, legt `auto_offset_reset="earliest"` fest, dass Kafka ab Offset 0 beginnt und alle vorhandenen Nachrichten liest. Mit `auto_offset_reset="latest"` werden stattdessen nur Nachrichten gelesen, die nach dem Start des Consumers eintreffen.

---

## Warum hat der Consumer beim zweiten Start keine alten Nachrichten mehr gelesen?

**Meine Antwort:**

Beim ersten Start war für die Consumer Group noch kein Offset gespeichert. Deshalb begann der Consumer bei Offset 0 und las alle vorhandenen Nachrichten. Anschließend speicherte Kafka den aktuellen Offset der Consumer Group. Beim zweiten Start wurde dieser gespeicherte Offset verwendet, sodass nur neue Nachrichten gelesen werden und die alten nicht erneut.

---

## Warum liest die Consumer Group `statistics` die alten Nachrichten erneut?

**Meine Antwort:**

Die Consumer Group `statistics` ist unabhängig von der Consumer Group `live-ticker`. Für `statistics` ist noch kein Offset gespeichert. Deshalb beginnt Kafka aufgrund von `auto_offset_reset="earliest"` bei Offset 0 und liest alle vorhandenen Nachrichten.

---

## Warum verschicken wir JSON statt einfachen Text?

**Meine Antwort:**

JSON enthält strukturierte Daten. Dadurch kann eine Nachricht mehrere Informationen enthalten, z. B. Spiel, Minute, Ereignis und Spieler. Jeder Consumer kann dann genau die Informationen verwenden, die er für seine Aufgabe benötigt.

---

## Was macht ein Serializer und ein Deserializer?

**Meine Antwort:**

Ein Serializer wandelt Daten in ein Format um, das übertragen oder gespeichert werden kann. Ein Deserializer wandelt diese Daten nach dem Empfangen wieder in die ursprüngliche Datenstruktur um, sodass das Programm damit arbeiten kann.

---

## Bringt ein Key bei nur einer Partition einen Vorteil für die Verteilung?

**Meine Antwort:**

Nein. Wenn ein Topic nur eine Partition hat, landen alle Nachrichten unabhängig vom Key in dieser einen Partition. Der Key kann die Last also erst dann auf unterschiedliche Partitionen verteilen, wenn mehrere Partitionen vorhanden sind.

---

## Warum können unterschiedliche Keys trotzdem in derselben Partition landen?

**Meine Antwort:**

Verschiedene Keys können in derselben Partition landen, weil Kafka den Key nach einer festen Regel auf die vorhandenen Partitionen abbildet. Da es nur wenige Partitionen, aber sehr viele mögliche Keys gibt, können unterschiedliche Keys auf dieselbe Partition fallen.

---

## Warum werden Offsets pro Partition gezählt?

**Meine Antwort:**

Offsets werden pro Partition gezählt, weil jede Partition ihre eigene Reihenfolge von Nachrichten hat. Ein Consumer muss für jede Partition einzeln wissen, bis zu welcher Nachricht er bereits gelesen hat. Ein einziger Offset für das gesamte Topic würde diese Information nicht eindeutig liefern.

---

## Warum teilt Kafka eine Partition nicht auf zwei Consumer derselben Consumer Group auf?

**Meine Antwort:**

Kafka teilt eine Partition nicht auf zwei Consumer derselben Consumer Group auf, weil sonst die Reihenfolge der Nachrichten nicht mehr garantiert werden könnte. Indem eine Partition immer nur von einem Consumer gelesen wird, bleibt die Reihenfolge der Nachrichten innerhalb dieser Partition erhalten.



---

## Was ist für bestehende Consumer sicherer: ein neues optionales Feld hinzufügen oder ein bestehendes Feld umbenennen?

**Meine Antwort:**

Ein neues optionales Feld hinzuzufügen ist sicherer, weil bestehende Consumer weiterhin die Felder finden, die sie bereits erwarten. Das neue Feld können sie einfach ignorieren. Wird dagegen player in scorer umbenannt, können ältere Consumer fehlschlagen, weil sie weiterhin nach player suchen.


---

## Warum wäre unser aktuelles ReplicationFactor: 1 für ein wichtiges Produktivsystem riskant?

**Meine Antwort:** 

Bei ReplicationFactor: 1 existiert von einer Partition keine weitere Replica auf einem anderen Broker. Fällt der Broker aus, ist die Partition nicht verfügbar und es besteht je nach Ausfallszenario auch ein Risiko für Datenverlust. Bei mehreren Replicas kann eine andere Replica die Leader-Rolle übernehmen.


---

## Was könnte passieren, wenn ein Consumer 10 Tage ausfällt, Kafka die Nachrichten aber nur 7 Tage aufbewahrt?

**Meine Antwort:** 

Wenn ein Consumer 10 Tage ausfällt, Kafka Nachrichten aber nur 7 Tage aufbewahrt, sind die ältesten Nachrichten bereits gelöscht. Der Consumer kann nach seiner Rückkehr nur noch die Nachrichten verarbeiten, die innerhalb der Retention noch vorhanden sind.


---

## Warum könnte es nützlich sein, dass Kafka eine Nachricht nach dem Lesen nicht sofort löscht?

**Meine Antwort:**

Kafka löscht Nachrichten nicht automatisch, nachdem ein Consumer sie gelesen hat. Dadurch können beispielsweise später hinzugefügte Consumer Groups alte Nachrichten ebenfalls verarbeiten oder Daten erneut verarbeitet werden.

---

## Warum kann At-least-once + Idempotenz besser sein, als Nachrichten vor der Verarbeitung zu committen?

**Meine Antwort:**

At-least-once + Idempotenz ist besser als Commit vor Verarbeitung, weil keine Nachricht verloren gehen soll. Eine Nachricht darf lieber erneut geliefert werden, solange die Verarbeitung so gebaut ist, dass ein Duplikat keinen doppelten fachlichen Effekt verursacht.


---

## Warum können bei At-least-once Duplikate entstehen?

**Meine Antwort:**

Ein Consumer kann eine Nachricht bereits erfolgreich verarbeitet haben, aber abstürzen, bevor der neue Offset committed wurde. Kafka weiß dann nicht, dass die Nachricht schon verarbeitet wurde, und liefert sie erneut aus. Dadurch kann dieselbe Nachricht mehrmals verarbeitet werden.

---

## Warum ist Rebalancing nützlich, wenn ein Consumer abstürzt?

**Meine Antwort:**

Rebalancing ist wichtig, weil beim Ausfall eines Consumers dessen Partitionen automatisch auf die verbleibenden Consumer derselben Consumer Group verteilt werden. Dadurch können die Nachrichten dieser Partitionen weiterhin verarbeitet werden.

---

## Warum bekommt C1 nicht einfach ungefähr die Hälfte aller 20 Nachrichten und C2 die andere Hälfte?

**Meine Antwort:**

C1 und C2 bekommen nicht einfach jeweils ungefähr die Hälfte aller Nachrichten, weil Kafka innerhalb einer Consumer Group ganze Partitionen auf Consumer verteilt. Würden Nachrichten derselben Partition auf mehrere Consumer aufgeteilt, könnte die Reihenfolge der Verarbeitung nicht mehr zuverlässig erhalten bleiben.

---

## Warum sollten Producer und Consumer sich auf ein gemeinsames Nachrichtenformat einigen?

**Meine Antwort:**

Producer und Consumer müssen sich auf ein gemeinsames Nachrichtenformat einigen, damit der Consumer die empfangenen Daten korrekt verarbeiten kann. Erwartet der Consumer beispielsweise JSON, der Producer sendet aber einfachen Text, kann der Consumer die Nachricht nicht interpretieren und es kommt zu einem Fehler.

---

Warum sollten live-ticker und statistics unterschiedliche Consumer Groups haben?

Beide Consumer Groups verfolgen unterschiedliche Aufgaben, sollen aber alle Events unabhängig voneinander erhalten. Deshalb verwenden sie unterschiedliche Consumer Groups. Würden beide Consumer zur selben Group gehören, würden die Partitionen und damit die Events zwischen ihnen aufgeteilt.

---

## Was bedeutet Forward Compatibility bei Schema Evolution?

**Meine Antwort:**

Forward Compatibility bedeutet, dass ein älterer Consumer auch neuere Nachrichten weiterhin verarbeiten kann. Wird beispielsweise ein neues optionales Feld hinzugefügt, kann ein alter Consumer dieses Feld ignorieren und weiterhin die ihm bekannten Felder verwenden.

---

## Was bedeutet Backward Compatibility bei Schema Evolution?

**Meine Antwort:**

Backward Compatibility bedeutet, dass ein neuer Consumer auch ältere Nachrichten weiterhin verarbeiten kann. Der neue Consumer muss also mit Nachrichten umgehen können, die noch nach einer älteren Version des Schemas aufgebaut sind.

---

## Was ist der Unterschied zwischen unserem bisherigen producer.py und dem Producer über FastAPI?

**Meine Antwort:**

Bei `producer.py` waren Anzahl und Inhalt der Events fest im Skript definiert. Mit FastAPI können andere Anwendungen per HTTP-POST dynamisch neue Events senden. Kafka bleibt dabei gleich – nur die Art, wie der Producer seine Daten erhält, ändert sich.

---

## Warum ist FastAPI vor Kafka nützlich?

**Meine Antwort:**

Mit FastAPI müssen andere Anwendungen Kafka nicht direkt kennen oder auf die Python-Producer-Datei zugreifen. Sie müssen nur den HTTP-Endpunkt und das erwartete Schema kennen und können darüber Events senden. Dadurch können mehrere unterschiedliche Anwendungen dieselbe Schnittstelle verwenden, während FastAPI intern die Kafka-Kommunikation übernimmt.

---

## Was ist das Pydantic-Modell in unserer FastAPI-Anwendung?

**Meine Antwort:**

`FootballEvent` ist unser Pydantic-Modell, weil die Klasse von `BaseModel` aus Pydantic erbt. Darin definieren wir, welche Felder und Datentypen ein eingehendes Event haben soll. FastAPI kann damit eingehende Requests validieren und beispielsweise einen Request mit `minute = "hallo"` ablehnen, weil `minute` als Integer definiert ist.

---

## Warum ist es sinnvoll, Daten bereits in FastAPI zu validieren, bevor sie an Kafka gesendet werden?

**Meine Antwort:**

Fehlerhafte Daten werden dadurch möglichst früh abgefangen und gar nicht erst in Kafka geschrieben. Dadurch müssen nachgelagerte Consumer nicht mit offensichtlich ungültigen Nachrichten umgehen.

---

## Warum brauchen wir PostgreSQL zusätzlich zu Kafka?

**Meine Antwort:**

Kafka speichert die Event-Historie und zusätzlich den Lesefortschritt der Consumer Groups über Offsets. Der aus den Events berechnete Zustand, zum Beispiel die Anzahl der Tore pro Team, lag bei uns zunächst nur im Arbeitsspeicher des Python-Programms und ging beim Neustart verloren. PostgreSQL verwenden wir, um diesen berechneten Zustand dauerhaft zu speichern.

---

## Was ist der Unterschied zwischen einem Kafka-Offset und unserem berechneten Zustand?

**Meine Antwort:**

Der Offset beschreibt den Lesefortschritt einer Consumer Group innerhalb einer Partition. Er beantwortet also vereinfacht die Frage: „Bis wohin habe ich gelesen?“

Der berechnete Zustand ist dagegen das Ergebnis der Verarbeitung dieser Nachrichten, zum Beispiel `Bayern = 2 Tore`. Dieser Zustand wird nicht automatisch durch den Kafka-Offset gespeichert und muss bei uns deshalb beispielsweise in PostgreSQL persistiert werden.

---

## Warum ging unsere Statistik vor PostgreSQL nach einem Neustart verloren?

**Meine Antwort:**

Die Torstatistik wurde nur in einer Python-Datenstruktur im Arbeitsspeicher gespeichert. Beim Beenden des Prozesses wurde dieser Arbeitsspeicher verworfen. Kafka kannte danach weiterhin den Lesefortschritt der Consumer Group, aber nicht mehr den daraus berechneten Torstand.

---

## Was ist ein Primary Key und warum verwenden wir team als Primary Key?

**Meine Antwort:**

Ein Primary Key identifiziert einen Datensatz innerhalb einer Tabelle eindeutig. In `team_statistics` verwenden wir `team` als Primary Key, damit beispielsweise `Bayern` nur einmal als Statistikzeile existiert und diese Zeile bei weiteren Bayern-Toren aktualisiert werden kann.

---

## Was bedeutet %s in unseren PostgreSQL-Abfragen?

**Meine Antwort:**

`%s` ist ein Platzhalter für einen Wert, den Python separat an PostgreSQL übergibt. Beispielsweise wird der Wert der Python-Variable `team` an dieser Stelle eingesetzt. Diese Parameterübergabe ist außerdem sicherer, als SQL-Abfragen selbst aus Strings zusammenzubauen, und schützt unter anderem vor SQL-Injection.

---

## Was macht INSERT ... ON CONFLICT ... DO UPDATE?

**Meine Antwort:**

Wenn ein Team noch nicht in `team_statistics` existiert, wird eine neue Zeile mit einem Tor angelegt. Existiert das Team aufgrund des Primary Keys bereits, entsteht ein Konflikt und `DO UPDATE` erhöht stattdessen den vorhandenen Torzähler um 1.

Diese Kombination aus Einfügen und Aktualisieren wird häufig als Upsert bezeichnet.

---

## Welches Problem kann zwischen Kafka und PostgreSQL bei At-least-once entstehen?

**Meine Antwort:**

Der Consumer kann ein Kafka-Event erfolgreich verarbeiten und den berechneten Zustand in PostgreSQL speichern, aber anschließend abstürzen, bevor der Kafka-Offset committed wurde. Nach dem Neustart kann Kafka dasselbe Event erneut liefern. Würde der Consumer einfach wieder `goals + 1` ausführen, würde dasselbe Tor doppelt gezählt.

---

## Warum verwenden wir zusätzlich eine event_id?

**Meine Antwort:**

Die `event_id` identifiziert ein konkretes Event eindeutig. Dadurch kann der Consumer überprüfen, ob dieses Event bereits verarbeitet wurde. Wird dieselbe Kafka-Nachricht erneut verarbeitet, kann die Anwendung erkennen, dass die `event_id` bereits bekannt ist, und die fachliche Aktion nicht noch einmal ausführen.

---

## Ist die event_id dasselbe wie der Kafka-Key?

**Meine Antwort:**

Nein. In unserem Projekt verwenden wir beispielsweise `Bayern` als Kafka-Key, damit zusammengehörige Events nach der Kafka-Partitionierungslogik derselben Partition zugeordnet werden. Die `event_id` identifiziert dagegen ein einzelnes konkretes Event eindeutig.

Beispiel:

- Kafka-Key: `Bayern`
- event_id: `goal-001`

Bayern kann viele unterschiedliche Events haben, die jeweils eine eigene `event_id` besitzen.

---

## Warum speichern wir verarbeitete event_ids in einer eigenen Tabelle?

**Meine Antwort:**

Ein Team kann viele unterschiedliche Events besitzen. In `team_statistics` soll trotzdem nur eine Zeile pro Team existieren. Deshalb speichern wir die bereits verarbeiteten Events separat in `processed_events`. So kann der Consumer prüfen, ob eine bestimmte `event_id` bereits verarbeitet wurde.

---

## Warum ist event_id in processed_events ein Primary Key?

**Meine Antwort:**

Jede `event_id` soll eindeutig sein und nur einmal in `processed_events` vorkommen. Der Primary Key erzwingt diese Eindeutigkeit und ermöglicht außerdem eine effiziente Suche nach einer bestimmten `event_id`.

---

## Wie verhindert unser Statistics Consumer die doppelte Verarbeitung eines Events?

**Meine Antwort:**

Der Consumer liest die `event_id` aus dem Kafka-Event und prüft zunächst in PostgreSQL, ob diese ID bereits in `processed_events` vorhanden ist. Ist sie bereits vorhanden, wird die Torstatistik nicht erneut verändert. Ist sie noch nicht vorhanden, wird die Statistik aktualisiert und die `event_id` anschließend als verarbeitet gespeichert.

Dadurch kann ein Event mehrfach aus Kafka gelesen werden, ohne dass es mehrfach fachlich ausgeführt wird.

---

## Warum führen wir Statistik-Update und Speichern der event_id in derselben Datenbanktransaktion aus?

**Meine Antwort:**

Beide Änderungen gehören fachlich zusammen. Würde zuerst der Torzähler erhöht und der Consumer danach vor dem Speichern der `event_id` abstürzen, könnte dasselbe Event später erneut gezählt werden.

Durch eine gemeinsame Datenbanktransaktion werden entweder beide Änderungen erfolgreich gespeichert oder bei einem Fehler beide zurückgerollt.

---

## Was ist der Unterschied zwischen COMMIT und ROLLBACK in PostgreSQL?

**Meine Antwort:**

Mit `COMMIT` werden die Änderungen einer Datenbanktransaktion dauerhaft gespeichert. Mit `ROLLBACK` werden die noch nicht committeden Änderungen der Transaktion zurückgenommen, wenn beispielsweise während der Verarbeitung ein Fehler auftritt.

---

## Warum kann dasselbe Event im Live-Ticker mehrfach erscheinen, obwohl unsere Statistik es nur einmal zählt?

**Meine Antwort:**

Kafka kann dasselbe Event mehrfach enthalten beziehungsweise ausliefern. Unser Live-Ticker zeigt jede empfangene Kafka-Nachricht an und besitzt keine Duplikatprüfung anhand der `event_id`.

Der Statistics Consumer arbeitet dagegen idempotent: Er prüft die `event_id` in PostgreSQL und verändert die Statistik nur beim ersten Verarbeiten dieses Events.

---

## Warum lesen wir GET /statistics aus PostgreSQL und nicht direkt aus Kafka?

**Meine Antwort:**

PostgreSQL speichert den bereits berechneten aktuellen Zustand dauerhaft. Kafka speichert dagegen die Event-Historie. Würden wir die aktuelle Statistik direkt aus Kafka erzeugen, müssten wir die Events zunächst erneut verarbeiten und daraus die Torzahlen berechnen.

PostgreSQL kann den bereits berechneten Zustand direkt liefern.

---

## Wie sieht unser aktueller Datenfluss aus?

**Meine Antwort:**

Neue Events werden per HTTP-POST an FastAPI gesendet. FastAPI validiert die Daten und veröffentlicht das Event in Kafka. Consumer lesen die Kafka-Events und können unterschiedliche Aufgaben ausführen. Unser Statistics Consumer verarbeitet Tor-Events und speichert den berechneten Zustand dauerhaft in PostgreSQL. Über `GET /statistics` kann FastAPI diesen Zustand anschließend wieder aus PostgreSQL auslesen.

Vereinfacht:

`POST /events → FastAPI → Kafka → Consumer → PostgreSQL → GET /statistics`

---

## ETL

### Was bedeutet ETL?

**Meine Antwort:**  
ETL steht für Extract, Transform und Load. Daten werden zuerst aus einer Quelle gelesen, anschließend bereinigt, vereinheitlicht oder angereichert und danach in ein Zielsystem geladen. In meinem Projekt lese ich Fußball-Events aus Kafka, transformiere die Daten und speichere sie anschließend in PostgreSQL.

### Wo befinden sich Extract, Transform und Load in meinem Projekt?

**Meine Antwort:**  
Extract ist das Lesen der Events aus Kafka im Statistics Consumer. Beim Transform bereinige ich zum Beispiel Team- und Spielernamen und leite aus der Spielminute eine neue Kategorie wie `first_half`, `second_half` oder `stoppage_time` ab. Beim Load speichere ich die transformierten Daten in PostgreSQL.

### Was kann bei einem Transform-Schritt passieren?

**Meine Antwort:**  
Ein Transform-Schritt kann Daten zum Beispiel bereinigen, vereinheitlichen, filtern oder aus vorhandenen Daten neue Informationen ableiten. In meinem Projekt wird zum Beispiel `" bayern "` zu `"Bayern"` und aus Minute 93 wird `stoppage_time`.

### Warum kann ein abgeleitetes Feld wie `match_phase` sinnvoll sein?

**Meine Antwort:**  
Das Feld ist leichter zu interpretieren und ermöglicht eine einfachere Gruppierung für spätere Analysen. Statt einzelne Minuten auszuwerten, kann ich zum Beispiel direkt zählen, wie viele Tore in der ersten Halbzeit, zweiten Halbzeit oder Nachspielzeit gefallen sind.

### Muss das Ergebnis einer Transformation wieder in der eingehenden JSON stehen?

**Meine Antwort:**  
Nein. Die JSON ist bei meinem Projekt zunächst die eingehende Nachricht. Wenn ich eine neu berechnete Information später verwenden möchte, muss ich sie beim Load in einem geeigneten Zielsystem speichern. Ich speichere `match_phase` deshalb zusätzlich in PostgreSQL.

### Warum speichere ich Events zusätzlich in PostgreSQL, obwohl sie bereits in Kafka liegen?

**Meine Antwort:**  
Kafka speichert den Event-Stream beziehungsweise die Ereignishistorie. PostgreSQL eignet sich besser dafür, daraus erzeugte Zustände und analysierbare Daten dauerhaft abzulegen und mit SQL abzufragen. In meinem Projekt kann ich dadurch zum Beispiel Tore nach `match_phase` gruppieren.

### Ist Kafka selbst ein ETL-Tool?

**Meine Antwort:**  
Nicht direkt. Kafka transportiert und speichert Events und kann damit eine wichtige Komponente einer ETL- oder Streaming-Pipeline sein. Die eigentliche Transformation übernimmt in meinem Projekt mein Python-Consumer.

### Kann ETL nur als Batch-Verarbeitung durchgeführt werden?

**Meine Antwort:**  
Nein. ETL kann auch kontinuierlich beziehungsweise als Streaming-Pipeline laufen. Mein Consumer verarbeitet neue Kafka-Events laufend, transformiert sie und schreibt sie anschließend nach PostgreSQL.

### Wie habe ich meine ETL-Pipeline praktisch getestet?

**Meine Antwort:**  
Ich habe absichtlich uneinheitliche Daten wie `" GOAL "`, `" Kane "` und `"bayern"` über die API geschickt. Der Consumer hat sie normalisiert und zusätzlich aus der Minute eine `match_phase` erzeugt. Danach habe ich die transformierten Events in PostgreSQL gespeichert und mit `GROUP BY match_phase` aggregiert.
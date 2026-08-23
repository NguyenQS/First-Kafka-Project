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
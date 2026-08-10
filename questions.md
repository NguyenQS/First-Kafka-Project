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
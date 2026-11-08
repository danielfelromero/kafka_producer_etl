# Kafka Producer ETL — Transactional Outbox Lab

Laboratorio didáctico para mostrar cómo una inserción en una base de datos puede originar un evento que posteriormente se publica en Kafka.

## Arquitectura

```text
insert_record.py
      |
      v
SQLite: records
      |
      | trigger AFTER INSERT
      v
SQLite: outbox (evento pendiente)
      |
      | producer.py consulta eventos pendientes
      v
Kafka topic: etl.records
      |
      v
Consumidores independientes
```

La base de datos SQLite se inicializa sin registros de negocio. Al insertar un registro en `records`, un trigger de SQLite crea automáticamente una fila en `outbox`, dentro de la misma transacción. El productor consulta la tabla `outbox`, publica los eventos pendientes en Kafka y los marca como publicados después de recibir la confirmación de entrega.

Este diseño introduce el patrón **Transactional Outbox** de forma sencilla. El trigger no inicia directamente un proceso Python: crea de forma transaccional el evento pendiente; el productor lo detecta mediante polling.

## Requisitos

- Python 3.10+
- Apache Kafka accesible por TCP desde el entorno del productor
- `confluent-kafka`

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 1. Inicializar la base de datos vacía

```bash
python init_db.py
```

Crea `data/events.db`, las tablas `records` y `outbox`, y el trigger `trg_records_after_insert`. No inserta registros de negocio.

Puedes comprobarlo con:

```bash
python -c "import sqlite3; c=sqlite3.connect('data/events.db'); print('records:', c.execute('select count(*) from records').fetchone()[0]); print('outbox:', c.execute('select count(*) from outbox').fetchone()[0])"
```

Ambos conteos deben ser cero en una inicialización nueva.

## 2. Preparar Kafka

El ejemplo asume un broker accesible en `localhost:9092` y un topic llamado `etl.records`.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic etl.records --partitions 3 --replication-factor 1
```

Para cambiar los parámetros, utiliza las variables de entorno descritas más adelante.

## 3. Iniciar el productor

En una terminal:

```bash
python producer.py
```

El productor permanece activo y consulta la tabla `outbox` cada segundo. Solo intenta publicar los eventos cuyo estado sea `pending`.

## 4. Insertar registros

En otra terminal:

```bash
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
python insert_record.py --station ST-02 --temperature 31.8 --humidity 83.0 --status alert
```

También puedes ejecutar `python insert_record.py` sin argumentos para utilizar el modo interactivo.

Al insertar una fila en `records`, el trigger crea la fila relacionada en `outbox`. El productor la detecta y la publica en Kafka. Para inspeccionar las dos tablas:

```bash
python -c "import sqlite3; c=sqlite3.connect('data/events.db'); print('records:', c.execute('select * from records').fetchall()); print('outbox:', c.execute('select * from outbox').fetchall())"
```

## 5. Estructura del evento Kafka

```json
{
  "event_id": 1,
  "record_id": 1,
  "event_type": "record.created",
  "station_id": "ST-01",
  "temperature": 27.4,
  "humidity": 71.2,
  "status": "normal",
  "created_at": "timestamp UTC"
}
```

El productor usa `station_id` como clave Kafka. Incluye también headers `event_type` y `event_id`.

## Configuración mediante variables de entorno

- `DB_PATH`: ruta de SQLite (por defecto `data/events.db`)
- `KAFKA_BOOTSTRAP_SERVERS`: broker Kafka (por defecto `localhost:9092`)
- `KAFKA_TOPIC`: topic (por defecto `etl.records`)
- `POLL_INTERVAL_SECONDS`: frecuencia de consulta (por defecto `1`)
- `BATCH_SIZE`: máximo de eventos consultados por ciclo (por defecto `100`)

Ejemplo:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC=etl.records python producer.py
```

## Reiniciar el laboratorio

Detén el productor con Ctrl+C y elimina la base SQLite:

```bash
rm -f data/events.db
python init_db.py
```

En una base nueva, ambas tablas estarán vacías y el trigger estará creado.

## Semántica y límites

- El trigger escribe en `outbox` dentro de la misma transacción que inserta el registro de negocio. Si la inserción falla, no debe quedar el evento correspondiente.
- Si Kafka no está disponible, el evento permanece pendiente y el productor vuelve a intentarlo.
- El productor marca un evento como publicado después de recibir la confirmación de Kafka.
- Si el proceso cae después de que Kafka acepta el mensaje, pero antes de actualizar SQLite, el evento puede publicarse otra vez. Es una semántica de entrega **al menos una vez**, no exactamente una vez de extremo a extremo.
- Los consumidores pueden deduplicar usando `event_id` o `record_id`.
- Este ejemplo supone una única instancia del productor. Ejecutar varias instancias requiere incorporar reclamación/lease de eventos pendientes para evitar publicaciones concurrentes.
- El polling añade una latencia aproximada relacionada con `POLL_INTERVAL_SECONDS`. No es un mecanismo de captura de cambios de alto rendimiento.

## Preguntas de discusión

1. ¿Qué responsabilidad cumple el trigger y cuál cumple el productor?
2. ¿Por qué separar `records` y `outbox`?
3. ¿Qué sucede si Kafka está caído al insertar un registro?
4. ¿Por qué puede haber duplicados y cómo los evita el consumidor?
5. ¿Qué se ganaría y qué complejidad añadiría utilizar CDC con Debezium?

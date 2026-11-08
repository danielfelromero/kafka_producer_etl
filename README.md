# Kafka Producer ETL — Database Change Polling Lab

Laboratorio para observar cómo una nueva fila en una base de datos puede activar la publicación de un evento en Kafka.

## Arquitectura

```text
insert_record.py -> SQLite (records) -> producer.py -> Kafka topic etl.records
```

La base SQLite se inicializa sin registros de negocio. El productor consulta periódicamente los registros con ID superior al último checkpoint y publica cada uno en Kafka.

**Nota conceptual:** esta primera versión usa polling incremental; no es CDC nativo basado en WAL/binlog. Es una decisión intencional para aprender el patrón sin instalar un servidor adicional. En producción se pueden estudiar herramientas como Debezium y conectores CDC.

## Requisitos

- Python 3.10+
- Broker Kafka accesible por TCP desde donde se ejecute el productor
- Python package `confluent-kafka`

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Inicializar la base vacía

```bash
python init_db.py
```

Crea `data/events.db` y la tabla `records`, sin insertar filas.

## Kafka

Se espera el broker en `localhost:9092` y el topic `etl.records`.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic etl.records --partitions 3 --replication-factor 1
```

Para una clase en la que el broker esté en el Codespace del docente y los alumnos se conecten desde otros entornos, debe configurarse un endpoint TCP externo y `advertised.listeners` correctamente. No asumir que el port forwarding HTTP habitual de Codespaces permite transportar el protocolo Kafka.

## Ejecutar el productor detector

En una terminal:

```bash
python producer.py
```

El productor permanece activo y consulta la base cada segundo.

Variables opcionales:
- `DB_PATH`: base SQLite (por defecto `data/events.db`)
- `KAFKA_BOOTSTRAP_SERVERS`: broker (por defecto `localhost:9092`)
- `KAFKA_TOPIC`: topic (por defecto `etl.records`)
- `POLL_INTERVAL_SECONDS`: intervalo de consulta (por defecto `1`)
- `CHECKPOINT_PATH`: checkpoint persistente (por defecto `data/producer_checkpoint.json`)

## Insertar registros

En otra terminal:

```bash
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
python insert_record.py --station ST-02 --temperature 31.8 --humidity 83.0 --status alert
```

Cada ejecución inserta una fila. El productor detecta el nuevo ID y lo publica en Kafka. También es posible ejecutar `python insert_record.py` sin argumentos y usar el modo interactivo.

## Evento publicado

```json
{
  "record_id": 1,
  "station_id": "ST-01",
  "temperature": 27.4,
  "humidity": 71.2,
  "status": "normal",
  "created_at": "timestamp UTC",
  "event_type": "record.created"
}
```

Los valores son ilustrativos; el script genera el timestamp real.

## Reiniciar el laboratorio

Detén el productor con Ctrl+C y elimina la base y el checkpoint:

```bash
rm -f data/events.db data/producer_checkpoint.json
python init_db.py
```

## Semántica de entrega

El checkpoint avanza únicamente después de que Kafka confirme la entrega. Si el productor cae después de que Kafka recibió el evento, pero antes de guardar el checkpoint, el registro puede publicarse de nuevo al reiniciar. Por eso se trata de entrega **al menos una vez**. Los consumidores pueden deduplicar usando `record_id`.

Si el productor está detenido cuando se inserta un registro, lo publicará al volver a arrancar. La latencia esperada de detección depende del intervalo de polling.

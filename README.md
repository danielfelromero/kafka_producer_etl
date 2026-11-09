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
- Apache Kafka 3.9.1 (el script de inicio lo descarga en el workspace)
- Java compatible con la distribución de Kafka
- `confluent-kafka`

## 1. Descargar e iniciar Kafka (KRaft)

**Antes de ejecutar `producer.py`, debes iniciar el broker.** Este repositorio incluye scripts para descargar Apache Kafka, inicializar el almacenamiento KRaft de un solo nodo y levantar el servicio localmente en `localhost:9092`.

Desde la raíz del repositorio, ejecuta:

```bash
chmod +x setup_kafka.sh diagnose_kafka.sh stop_kafka.sh
./setup_kafka.sh
```

La primera ejecución descarga Apache Kafka 3.9.1 y puede tardar. Las siguientes reutilizan la distribución descargada. El script:

1. Descarga y descomprime Kafka en `kafka_2.13-3.9.1/`.
2. Inicializa el almacenamiento KRaft en `.kafka/data/` si todavía no existe.
3. Inicia el broker en segundo plano.
4. Espera a que el broker responda en `localhost:9092`.
5. Crea el topic `etl.records` con 3 particiones y factor de replicación 1.

El laboratorio utiliza **un único broker**, por lo que el factor de replicación debe ser 1. Tres particiones no significan tres brokers.

Para comprobar el broker y el topic:

```bash
./diagnose_kafka.sh
```

Para consultar los comandos nativos de Kafka, utiliza su ruta dentro de la distribución descargada. Por ejemplo:

```bash
./kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

./kafka_2.13-3.9.1/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic etl.records
```

**Nota:** `kafka-topics.sh` no se instala como comando global ni forma parte del entorno virtual de Python. Por eso escribir solamente `kafka-topics.sh` produce `command not found`. Debes invocarlo mediante la ruta anterior.

Para detener Kafka sin eliminar los datos:

```bash
./stop_kafka.sh
```

Los datos persistentes del broker permanecen en `.kafka/data/`. No elimines ese directorio para una detención normal.

## 2. Instalar las dependencias Python

En otra terminal o después de iniciar Kafka:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Inicializar la base de datos vacía

```bash
python init_db.py
```

Crea `data/events.db`, las tablas `records` y `outbox`, y el trigger `trg_records_after_insert`. No inserta registros de negocio.

Puedes comprobarlo con:

```bash
python -c "import sqlite3; c=sqlite3.connect('data/events.db'); print('records:', c.execute('select count(*) from records').fetchone()[0]); print('outbox:', c.execute('select count(*) from outbox').fetchone()[0])"
```

Ambos conteos deben ser cero en una inicialización nueva.

## 4. Iniciar el productor

Asegúrate de que Kafka esté activo y que el topic `etl.records` exista. En una terminal:

```bash
source .venv/bin/activate
python producer.py
```

El productor permanece activo y consulta la tabla `outbox` cada segundo. Solo intenta publicar los eventos cuyo estado sea `pending`.

En la salida inicial debe aparecer:

```text
Watching SQLite outbox: data/events.db
Kafka bootstrap: localhost:9092 | topic: etl.records
Waiting for pending events. Press Ctrl+C to stop.
```

Si aparecen errores `Connection refused`, comprueba primero que el broker esté levantado con `./diagnose_kafka.sh`. No es un error de SQLite ni del entorno virtual: significa que el cliente no puede conectarse al broker configurado.

## 5. Insertar registros

En otra terminal:

```bash
source .venv/bin/activate
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
python insert_record.py --station ST-02 --temperature 31.8 --humidity 83.0 --status alert
```

También puedes ejecutar `python insert_record.py` sin argumentos para utilizar el modo interactivo.

Al insertar una fila en `records`, el trigger crea la fila relacionada en `outbox`. El productor la detecta y la publica en Kafka. Para inspeccionar las dos tablas:

```bash
python -c "import sqlite3; c=sqlite3.connect('data/events.db'); print('records:', c.execute('select * from records').fetchall()); print('outbox:', c.execute('select * from outbox').fetchall())"
```

En la terminal del productor deberías observar un mensaje `PUBLISHED` con el identificador del evento, topic, partición y offset.

## 6. Estructura del evento Kafka

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

Detén el productor con Ctrl+C. Si quieres reiniciar únicamente la base de datos SQLite:

```bash
rm -f data/events.db
python init_db.py
```

Esto no elimina ni reinicia el broker Kafka. Para detener Kafka, utiliza `./stop_kafka.sh`. En una base nueva, ambas tablas estarán vacías y el trigger estará creado.

## Semántica y límites

- El trigger escribe en `outbox` dentro de la misma transacción que inserta el registro de negocio. Si la inserción falla, no debe quedar el evento correspondiente.
- Si Kafka no está disponible, el evento permanece pendiente y el productor vuelve a intentarlo.
- El productor marca un evento como publicado después de recibir la confirmación de Kafka.
- Si el proceso cae después de que Kafka acepta el mensaje, pero antes de actualizar SQLite, el evento puede publicarse otra vez. Es una semántica de entrega **al menos una vez**, no exactamente una vez de extremo a extremo.
- Los consumidores pueden deduplicar usando `event_id` o `record_id`.
- Este ejemplo supone una única instancia del productor. Ejecutar varias instancias requiere incorporar reclamación/lease de eventos pendientes para evitar publicaciones concurrentes.
- El polling añade una latencia aproximada relacionada con `POLL_INTERVAL_SECONDS`. No es un mecanismo de captura de cambios de alto rendimiento.
- Este laboratorio utiliza un broker local de un solo nodo, sin tolerancia a fallos ni configuración de acceso remoto para Colab.

## Preguntas de discusión

1. ¿Qué responsabilidad cumple el trigger y cuál cumple el productor?
2. ¿Por qué separar `records` y `outbox`?
3. ¿Qué sucede si Kafka está caído al insertar un registro?
4. ¿Por qué puede haber duplicados y cómo los evita el consumidor?
5. ¿Qué se ganaría y qué complejidad añadiría utilizar CDC con Debezium?

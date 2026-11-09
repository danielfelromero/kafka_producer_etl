# Kafka Producer ETL — Transactional Outbox Lab

Laboratorio didáctico para mostrar cómo una inserción en una base de datos puede originar un evento que posteriormente se publica en Kafka. Incluye un perfil local y un perfil aula para que el docente comparta un único broker con estudiantes conectados desde Google Colab.

## Arquitectura

```text
insert_record.py
      |
      v
SQLite: records
      |
      | trigger AFTER INSERT
      v
SQLite: outbox
      |
      | producer.py consulta eventos pendientes
      v
Kafka topic: etl.records
      |
      +------------------------------+
      |                              |
      v                              v
Consumer local                 Consumers de estudiantes
                               (Google Colab / Python)
```

El trigger de SQLite crea una fila en `outbox` dentro de la misma transacción que inserta el registro. El productor consulta los eventos pendientes, los publica en Kafka y los marca como publicados después de recibir la confirmación de entrega.

Este ejemplo implementa el patrón **Transactional Outbox** mediante polling. No es CDC nativo.

## Requisitos

- Python 3.10+
- Java compatible con Apache Kafka 3.9.1
- Apache Kafka 3.9.1 (el script lo descarga en el workspace)
- Para compartir Kafka: conexión saliente a `bore.pub` (no requiere cuenta ni tarjeta)
- `confluent-kafka`

---

# Parte A. Ejecutar Kafka localmente

Desde la raíz del repositorio:

```bash
chmod +x setup_kafka.sh diagnose_kafka.sh stop_kafka.sh start_classroom.sh start_ngrok_classroom.sh
./setup_kafka.sh
```

La primera ejecución descarga Apache Kafka 3.9.1, inicializa KRaft y crea el topic `etl.records` con tres particiones y factor de replicación 1.

El perfil predeterminado es `LOCAL`; Kafka queda disponible en `localhost:9092`.

Instala las dependencias e inicializa SQLite:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python init_db.py
```

Inicia el productor:

```bash
python producer.py
```

En otra terminal, inserta un registro:

```bash
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
```

Comprueba el broker y el topic:

```bash
./diagnose_kafka.sh
```

Los comandos nativos de Kafka no están instalados globalmente. Invócalos mediante su ruta:

```bash
./kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

# Parte B. Compartir el broker con estudiantes mediante Bore

## Objetivo

El docente ejecuta un único broker Kafka dentro de su Codespace. Bore crea un túnel TCP desde el puerto externo de Kafka hacia el servicio público `bore.pub`. Los estudiantes se conectan desde Google Colab con `confluent-kafka`, sin instalar agentes de túnel en sus notebooks.

```text
Docente / GitHub Codespace
  Producer -> localhost:9092
                  |
                Kafka
                  |
          localhost:19092
                  |
              Bore CLI
                  |
          bore.pub:<puerto>
                  |
       +----------+----------+
       |          |          |
     Colab 1    Colab 2    Colab N
     Consumer   Consumer   Consumer
```

## 1. Requisitos y costos

El perfil aula usa [Bore](https://github.com/ekzhang/bore), un cliente de túnel TCP de código abierto. El proyecto publica un servidor compartido en `bore.pub`. El cliente se descarga automáticamente desde los releases oficiales y se verifica mediante SHA-256.

- No se requiere cuenta, authtoken, tarjeta ni contratar un plan de ngrok.
- No se configura ni reserva un dominio.
- Bore asigna un puerto TCP público dinámico en `bore.pub`.
- El servicio `bore.pub` es una instancia pública comunitaria: no ofrece una garantía de disponibilidad para una clase o un entorno de producción.

La herramienta y su uso se documentan en:
- https://github.com/ekzhang/bore
- https://github.com/ekzhang/bore/releases

## 2. Advertencia sobre el acceso público

El perfil aula utiliza Kafka `PLAINTEXT`, sin SASL, TLS, autenticación, ACL ni cifrado de Kafka. El túnel Bore tampoco cifra los datos de aplicación por defecto.

Cualquier persona que conozca el host y puerto público puede intentar leer o publicar mensajes. No compartas información privada, credenciales, datos personales ni información sensible. Utiliza el endpoint solo durante una práctica temporal y detenlo al terminar.

El broker escucha en `127.0.0.1:19092` dentro del Codespace. Bore es el único mecanismo que publica ese puerto. El Producer del docente continúa conectado a `localhost:9092`.

## 3. Configurar el perfil CLASSROOM

Solo la primera vez:

```bash
cp .env.classroom.example .env.classroom
```

El archivo tendrá:

```dotenv
KAFKA_PROFILE=CLASSROOM
KAFKA_TOPIC=etl.records
```

No hay que agregar dominios, tokens ni contraseñas. `.env.classroom` está excluido de Git.

Si Kafka ya está activo en modo LOCAL, detenlo primero:

```bash
./stop_kafka.sh
```

## 4. Iniciar el entorno aula con un único comando

Ejecuta:

```bash
./start_classroom.sh
```

El script realiza automáticamente lo siguiente:

1. Descarga Bore v0.6.0 si no existe en `.tools/bore`.
2. Verifica el hash SHA-256 del binario oficial.
3. Abre un túnel TCP desde `127.0.0.1:19092` hacia `bore.pub`.
4. Obtiene el puerto público asignado por el servidor Bore.
5. Guarda el endpoint en `.kafka/tunnel_address`.
6. Genera `advertised.listeners` de Kafka con el host y puerto públicos.
7. Arranca Kafka en modo KRaft y crea/verifica el topic `etl.records`.

La salida será similar a:

```text
1/3 Abriendo túnel TCP con Bore hacia 127.0.0.1:19092...
2/3 Esperando la dirección TCP asignada por bore.pub...
Endpoint TCP asignado: bore.pub:54321
3/3 Iniciando Kafka en modo CLASSROOM...

ENTORNO DE AULA LISTO
Kafka local del docente: localhost:9092
Endpoint TCP para estudiantes: bore.pub:54321
Topic: etl.records
Seguridad Kafka: PLAINTEXT sin autenticación ni cifrado.
```

El puerto del ejemplo es ilustrativo; comparte el que aparezca en tu terminal.

**Mantén esta terminal abierta durante toda la sesión.** Presionar `Ctrl+C` detiene Kafka y Bore.

## 5. Configuración de Kafka en modo CLASSROOM

El script configura tres listeners:

| Listener | Dirección | Función |
|---|---|---|
| INTERNAL | `127.0.0.1:9092` | Producer y administración del docente |
| EXTERNAL | `127.0.0.1:19092` | Tráfico recibido desde Bore |
| CONTROLLER | `127.0.0.1:9093` | Quorum KRaft |

Kafka anuncia la dirección Bore real, por ejemplo:

```properties
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://bore.pub:54321
```

Los clientes que se conecten por el listener EXTERNAL recibirán esa dirección en la metadata de Kafka. Por eso deben utilizar el mismo endpoint público como bootstrap server.

## 6. Obtener y compartir la dirección con los estudiantes

El endpoint se puede consultar con:

```bash
cat .kafka/tunnel_address
```

Comparte el resultado, por ejemplo:

```text
bore.pub:54321
```

No compartas `localhost:9092`, porque esa dirección corresponde al Codespace del docente.

## 7. Consumer desde Google Colab

Los estudiantes solo necesitan instalar el cliente Kafka:

```python
!pip -q install confluent-kafka
```

Luego se conectan con el host y puerto que les compartiste:

```python
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "bore.pub:54321"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "PLAINTEXT",
    "group.id": "student-01",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False
})

metadata = consumer.list_topics(timeout=20)

print("Conexión establecida")
print("Cluster ID:", metadata.cluster_id)
print("Brokers anunciados:", {
    broker_id: f"{broker.host}:{broker.port}"
    for broker_id, broker in metadata.brokers.items()
})

consumer.subscribe(["etl.records"])
print("Suscrito a etl.records")
```

Después pueden consumir normalmente:

```python
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Error:", msg.error())
            continue

        print(msg.value().decode("utf-8"))

finally:
    consumer.close()
```

También se incluye `examples/classroom_consumer.py`. Para ejecutarlo, configura:

```bash
export KAFKA_BOOTSTRAP_SERVERS="bore.pub:54321"
export CONSUMER_GROUP="student-01"
python examples/classroom_consumer.py
```

## 8. Cada estudiante debe tener un group.id diferente

Para que cada estudiante reciba independientemente todos los mensajes, deben utilizar grupos distintos:

- `student-ana`
- `student-carlos`
- `student-03`

Si todos utilizan el mismo `group.id`, Kafka distribuye las particiones y los mensajes entre los integrantes del grupo.

Con `auto.offset.reset=latest`, un grupo nuevo comienza a recibir los mensajes publicados después de conectarse. Para leer los mensajes existentes, pueden usar `earliest` si el grupo aún no tiene offsets confirmados.

## 9. Enviar eventos desde el Producer del docente

El Producer continúa ejecutándose localmente y no requiere cambios:

```bash
source .venv/bin/activate
python producer.py
```

En otra terminal:

```bash
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
```

El evento se publica en `etl.records` y todos los grupos de consumidores independientes podrán recibirlo.

## 10. Diagnóstico

Comprueba el broker local, topic, particiones y offsets:

```bash
./diagnose_kafka.sh
```

Consulta los listeners configurados:

```bash
grep -E '^(listeners|advertised.listeners|inter.broker.listener.name|listener.security.protocol.map)=' .kafka/server.properties
```

Consulta el endpoint público:

```bash
cat .kafka/tunnel_address
```

El log de Bore está en:

```bash
tail -n 80 .kafka/bore.log
```

Si el Consumer puede llegar al bootstrap pero falla después al consultar metadata, revisa que Kafka anuncie exactamente `bore.pub:puerto`, que el proceso Bore siga activo y que el endpoint corresponda a la sesión actual.

## 11. Detener el laboratorio

En la terminal donde ejecutaste `./start_classroom.sh`, presiona `Ctrl+C`. El script detiene Kafka y Bore y elimina el endpoint local guardado.

En la siguiente sesión Bore asignará un puerto nuevo. Basta con volver a ejecutar el mismo comando y compartir la nueva dirección.

## 12. Compatibilidad con el comando anterior

Se conserva `start_ngrok_classroom.sh` como alias de compatibilidad. Aunque el nombre histórico menciona ngrok, ahora ejecuta el mismo flujo basado en Bore:

```bash
./start_ngrok_classroom.sh
```

Se recomienda utilizar `./start_classroom.sh` en adelante.

## Límites del laboratorio

- Un único broker KRaft, tres particiones y factor de replicación 1. No hay alta disponibilidad.
- El endpoint TCP es público y no requiere autenticación ni cifrado de Kafka.
- Cualquier persona que conozca la dirección puede intentar conectarse.
- El relay `bore.pub` es un servicio comunitario sin SLA de disponibilidad.
- El puerto público es dinámico; Kafka anuncia el endpoint vigente al iniciar.
- No utilices esta configuración en producción ni con datos sensibles.

## Preguntas de discusión

1. ¿Qué diferencia existe entre `listeners` y `advertised.listeners`?
2. ¿Por qué el túnel debe estar activo antes de iniciar Kafka?
3. ¿Qué ocurre si Bore asigna un puerto diferente al reiniciar?
4. ¿Por qué cada estudiante necesita un `group.id` diferente para recibir todos los mensajes?
5. ¿Qué riesgos aparecen al exponer Kafka sin autenticación?
6. ¿Qué ventajas ofrecería agregar TLS, SASL y ACL en un entorno real?

---

# Transactional Outbox: semántica y límites

- El trigger escribe en `outbox` dentro de la misma transacción que inserta el registro de negocio.
- Si Kafka no está disponible, el evento permanece pendiente y el productor vuelve a intentarlo.
- El productor marca un evento como publicado después de recibir la confirmación de Kafka.
- Si el proceso cae después de que Kafka acepta el mensaje, pero antes de actualizar SQLite, el evento puede publicarse otra vez. Es una semántica de entrega **al menos una vez**, no exactamente una vez de extremo a extremo.
- Los consumidores pueden deduplicar usando `event_id` o `record_id`.
- Este ejemplo supone una única instancia del productor. Ejecutar varias instancias requiere reclamación/lease de eventos pendientes.
- El polling añade latencia relacionada con `POLL_INTERVAL_SECONDS`; no es CDC de alto rendimiento.

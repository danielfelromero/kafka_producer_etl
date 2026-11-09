# Kafka Producer ETL — Transactional Outbox Lab

Laboratorio didáctico para mostrar cómo una inserción en una base de datos puede originar un evento que posteriormente se publica en Kafka. Incluye dos perfiles para ejecutar Kafka localmente o compartir un broker con estudiantes desde Google Colab.

## Arquitectura del laboratorio

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
Consumer local                 Consumers externos
                               (Google Colab / Python)
```

La base de datos SQLite se inicializa sin registros de negocio. Al insertar un registro en `records`, un trigger de SQLite crea automáticamente una fila en `outbox`, dentro de la misma transacción. El productor consulta la tabla `outbox`, publica los eventos pendientes en Kafka y los marca como publicados después de recibir la confirmación de entrega.

Este diseño introduce el patrón **Transactional Outbox**. El trigger no inicia directamente un proceso Python: crea de forma transaccional el evento pendiente; el productor lo detecta mediante polling.

## Requisitos

- Python 3.10+
- Java compatible con Apache Kafka 3.9.1
- Apache Kafka 3.9.1 (los scripts lo descargan en el workspace)
- Para el perfil CLASSROOM: cuenta de ngrok y un dominio TLS asignado a tu cuenta
- `confluent-kafka`

---

# Parte A. Kafka local en Codespaces

## 1. Descargar e iniciar Kafka (KRaft)

Desde la raíz del repositorio:

```bash
chmod +x setup_kafka.sh diagnose_kafka.sh stop_kafka.sh start_ngrok_classroom.sh
./setup_kafka.sh
```

La primera ejecución descarga Apache Kafka 3.9.1. El script inicializa el almacenamiento KRaft de un solo nodo, inicia el broker y crea el topic `etl.records` con tres particiones y factor de replicación 1.

El perfil predeterminado es `LOCAL`. El broker queda disponible en `localhost:9092`.

Comprueba la conexión y el topic:

```bash
./diagnose_kafka.sh
```

Los comandos nativos de Kafka no están instalados globalmente ni forman parte del entorno virtual de Python. Invócalos mediante la ruta completa:

```bash
./kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 2. Instalar dependencias e inicializar SQLite

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python init_db.py
```

La base de datos se crea vacía. Puedes comprobarlo:

```bash
python -c "import sqlite3; c=sqlite3.connect('data/events.db'); print('records:', c.execute('select count(*) from records').fetchone()[0]); print('outbox:', c.execute('select count(*) from outbox').fetchone()[0])"
```

## 3. Iniciar el productor e insertar eventos

Terminal 1:

```bash
source .venv/bin/activate
python producer.py
```

Terminal 2:

```bash
source .venv/bin/activate
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
```

El trigger crea el evento en `outbox`; el producer lo publica en `etl.records`. Deberías ver `PUBLISHED` junto con topic, partición y offset.

---

# Parte B. Compartir el broker con estudiantes mediante ngrok

Este perfil levanta **un único broker Kafka en tu Codespace** y publica un endpoint TLS para que varios estudiantes se conecten desde sus propios Colab. Los estudiantes no necesitan instalar Kafka, Docker, Java ni ngrok: solamente instalan el cliente Python.

## 4. Instalar ngrok en el Codespace

En el terminal del Codespace ejecuta la instalación oficial para Debian/Ubuntu:

```bash
curl -sSL https://ngrok-agent.s3.amazonaws.com/ngrok.asc \
  | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null

echo "deb https://ngrok-agent.s3.amazonaws.com bookworm main" \
  | sudo tee /etc/apt/sources.list.d/ngrok.list

sudo apt update
sudo apt install ngrok
```

Configura el authtoken desde tu cuenta de ngrok:

```bash
ngrok config add-authtoken TU_AUTHTOKEN
```

El authtoken es un secreto del administrador. No lo pongas en el repositorio, en el README ni en un notebook compartido.

Consulta la guía oficial: https://ngrok.com/docs/getting-started/

## 5. Asignar un dominio TLS en ngrok

En el dashboard de ngrok, entra a la sección **Domains** y utiliza un dominio asignado/reservado para tu cuenta que pueda servir un TLS Endpoint.

Copia únicamente el hostname. Por ejemplo:

```text
kafka-clase-tuusuario.ngrok.app
```

No incluyas `tls://`, `https://` ni un puerto.

El dominio debe ser el mismo que se utilizará en el comando ngrok y en el valor `advertised.listeners` de Kafka. Los TLS Endpoints utilizan SNI para seleccionar el endpoint y ngrok gestiona el certificado del dominio. Consulta: https://ngrok.com/docs/gateway/endpoints/tls

## 6. Crear la configuración privada del aula

Copia el archivo de ejemplo:

```bash
cp .env.classroom.example .env.classroom
```

Edita `.env.classroom` y reemplaza el hostname:

```dotenv
KAFKA_PROFILE=CLASSROOM
KAFKA_TOPIC=etl.records
NGROK_DOMAIN=kafka-clase-tuusuario.ngrok.app
```

Este archivo está incluido en `.gitignore`; no lo subas a GitHub.

Carga las variables y arranca Kafka en modo aula:

```bash
set -a
source .env.classroom
set +a

./stop_kafka.sh 2>/dev/null || true
./setup_kafka.sh
```

El script genera una configuración propia en `.kafka/server.properties` y conserva la configuración original descargada de Apache Kafka sin modificarla.

En modo `CLASSROOM` configura:

| Listener | Bind | Propósito |
|---|---|---|
| `INTERNAL` | `127.0.0.1:9092` | Producer y administración desde el Codespace |
| `EXTERNAL` | `127.0.0.1:19092` | Entrada local del agente ngrok |
| `CONTROLLER` | `127.0.0.1:9093` | Quorum KRaft |

Kafka anuncia:

```properties
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://kafka-clase-tuusuario.ngrok.app:443
```

El hostname será el de tu archivo `.env.classroom`.

El script crea un usuario SASL/PLAIN llamado `student` con una contraseña aleatoria guardada localmente en `.kafka/student_password`. También configura el authorizer de KRaft y ACLs para que ese usuario pueda leer y describir solamente el topic configurado y utilizar grupos cuyo nombre comience por `student-`.

El usuario de estudiantes **no tiene permiso de escritura** sobre el topic. El productor del docente sigue usando el listener interno `localhost:9092`, por lo que no requiere cambios.

## 7. Iniciar el endpoint TLS de ngrok

Con Kafka ya iniciado en modo `CLASSROOM`, ejecuta en otra terminal del Codespace:

```bash
./start_ngrok_classroom.sh
```

El script ejecuta el equivalente a:

```bash
ngrok tls 19092 \
  --url tls://kafka-clase-tuusuario.ngrok.app \
  --traffic-policy-file ./ngrok-classroom-policy.yml
```

El Traffic Policy utiliza la acción `terminate-tls`: el TLS del cliente termina en el edge de ngrok y ngrok reenvía el tráfico por su Secure Tunnel hasta el agente. El agente entrega el protocolo Kafka al listener externo SASL de Kafka en el puerto local 19092.

Mantén abierta esta terminal durante toda la clase. Si se detiene ngrok, los clientes externos dejan de poder conectarse.

**Importante sobre el cifrado:** el endpoint TLS protege el trayecto cliente → ngrok y el Secure Tunnel protege el trayecto ngrok → agente. En el último tramo, el agente conecta al listener SASL_PLAINTEXT de Kafka por loopback dentro del mismo Codespace. No es TLS de extremo a extremo hasta el proceso Kafka. Para un despliegue de producción, configura TLS en el propio listener Kafka y autentica también el trayecto hasta el broker.

## 8. Obtener la credencial que compartirás con los estudiantes

En el Codespace:

```bash
cat .kafka/student_password
```

El usuario es:

```text
student
```

Comparte la contraseña por un canal privado del curso; nunca la publiques en el repositorio, en un README público ni en un notebook compartido. Todos los estudiantes utilizarán esta credencial compartida en este laboratorio. Si necesitas revocar el acceso de una persona sin afectar al resto, habrá que crear identidades individuales en lugar de compartir un usuario.

## 9. Consumer desde Google Colab

Los estudiantes solamente necesitan ejecutar:

```python
!pip -q install confluent-kafka
```

Y configurar el consumer:

```python
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "kafka-clase-tuusuario.ngrok.app:443"
KAFKA_USERNAME = "student"
KAFKA_PASSWORD = "CONTRASEÑA_COMPARTIDA_POR_EL_DOCENTE"

# Cada estudiante debe usar un group.id diferente.
CONSUMER_GROUP = "student-01"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": KAFKA_USERNAME,
    "sasl.password": KAFKA_PASSWORD,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
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

El resto del consumo puede realizarse con el ciclo habitual de `consumer.poll()`. También se incluye un ejemplo ejecutable en `examples/classroom_consumer.py`.

### ¿Por qué cada estudiante debe tener un group.id distinto?

Para este ejercicio queremos que todos reciban los mismos eventos. Por eso cada estudiante usa, por ejemplo:

- `student-ana`
- `student-carlos`
- `student-03`

Con grupos distintos, cada grupo mantiene sus propios offsets y recibe independientemente los eventos. Si todos comparten el mismo `group.id`, Kafka distribuye las particiones y los mensajes entre los miembros del grupo.

Con `auto.offset.reset=latest`, un grupo nuevo espera eventos publicados después de iniciar la suscripción. Para leer los mensajes que ya existían al conectarse, se puede usar `earliest` si ese grupo todavía no tiene offsets guardados.

## 10. Verificación desde el Codespace

El docente puede seguir usando:

```bash
./diagnose_kafka.sh
```

La configuración de aula se puede inspeccionar con:

```bash
grep -E '^(listeners|advertised.listeners|inter.broker.listener.name|listener.security.protocol.map|authorizer.class.name|super.users)=' .kafka/server.properties
```

Comprueba que el listener externo existe:

```bash
ss -lnt | grep 19092
```

El endpoint ngrok debe permanecer activo y el hostname anunciado debe coincidir exactamente con `NGROK_DOMAIN`.

## 11. Detener el aula

1. Detén ngrok con Ctrl+C en la terminal donde ejecutaste `./start_ngrok_classroom.sh`.
2. Detén Kafka:

```bash
./stop_kafka.sh
```

Los datos de Kafka y la contraseña generada se conservan localmente en `.kafka/`. El archivo `.kafka/student_password` no se versiona.

Para volver al perfil local:

```bash
./stop_kafka.sh
unset NGROK_DOMAIN
KAFKA_PROFILE=LOCAL ./setup_kafka.sh
```

## Seguridad y límites del modo aula

- El broker es de **un solo nodo**, con tres particiones y factor de replicación 1. No hay alta disponibilidad.
- El listener externo está ligado a loopback y se alcanza mediante ngrok; no se publica directamente el puerto 19092 en la red.
- Los clientes externos usan SASL_SSL contra el endpoint TLS de ngrok. El listener Kafka externo usa SASL_PLAINTEXT detrás del edge y el túnel seguro. Consulta la documentación de TLS Endpoints y TLS Termination de ngrok.
- Todos los estudiantes comparten el usuario `student` en esta versión. Las ACL limitan ese principal a leer el topic y a utilizar consumer groups con prefijo `student-`. La contraseña es compartida; no permite revocar a un estudiante individualmente.
- El productor local y las operaciones administrativas utilizan el listener interno. En este laboratorio, el principal anónimo del listener interno se configura como superusuario; ese listener está ligado a loopback y no debe exponerse.
- No compartas el authtoken de ngrok ni la contraseña del consumidor en GitHub.
- Si el dominio de ngrok cambia, detén Kafka, actualiza `.env.classroom`, vuelve a ejecutar `setup_kafka.sh` y reinicia el endpoint ngrok. Kafka anuncia el dominio y puerto en metadata; no basta con cambiar únicamente el bootstrap del consumer.
- No utilices esta configuración como arquitectura de producción.

## Semántica del Transactional Outbox

- El trigger escribe en `outbox` dentro de la misma transacción que inserta el registro de negocio.
- Si Kafka no está disponible, el evento permanece pendiente y el productor vuelve a intentarlo.
- El productor marca un evento como publicado después de recibir la confirmación de Kafka.
- Si el proceso cae después de que Kafka acepta el mensaje, pero antes de actualizar SQLite, el evento puede publicarse otra vez. Es una semántica de entrega **al menos una vez**, no exactamente una vez de extremo a extremo.
- Los consumidores pueden deduplicar usando `event_id` o `record_id`.
- Este ejemplo supone una única instancia del productor. Ejecutar varias instancias requiere reclamación/lease de eventos pendientes.
- El polling añade latencia relacionada con `POLL_INTERVAL_SECONDS`; no es CDC de alto rendimiento.

## Preguntas de discusión

1. ¿Qué responsabilidad cumple el trigger y cuál cumple el productor?
2. ¿Por qué separar `records` y `outbox`?
3. ¿Qué sucede si Kafka está caído al insertar un registro?
4. ¿Por qué puede haber duplicados y cómo los evita el consumidor?
5. ¿Qué diferencia existe entre `listeners` y `advertised.listeners`?
6. ¿Por qué cada estudiante necesita un `group.id` distinto para recibir todos los eventos?
7. ¿Qué se ganaría y qué complejidad añadiría utilizar CDC con Debezium?

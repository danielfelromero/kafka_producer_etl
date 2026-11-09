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
- Para compartir Kafka: cuenta gratuita de ngrok y método de pago verificado para habilitar TCP
- `confluent-kafka`

---

# Parte A. Ejecutar Kafka localmente

Desde la raíz del repositorio:

```bash
chmod +x setup_kafka.sh diagnose_kafka.sh stop_kafka.sh start_ngrok_classroom.sh
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

# Parte B. Compartir el broker con estudiantes mediante ngrok TCP

## Objetivo

El docente ejecuta un único broker Kafka dentro de su Codespace. ngrok crea un endpoint TCP público que redirige las conexiones al listener externo de Kafka. Los estudiantes se conectan desde sus propios Colab utilizando únicamente `confluent-kafka`.

```text
Docente / GitHub Codespace
  Producer -> localhost:9092
                  |
                Kafka
                  |
          localhost:19092
                  |
             ngrok agent
                  |
          endpoint TCP público
                  |
       +----------+----------+
       |          |          |
     Colab 1    Colab 2    Colab N
     Consumer   Consumer   Consumer
```

## 1. Importante: límites del plan gratuito de ngrok

El error `ERR_NGROK_312` apareció porque el script anterior intentaba crear un **TLS Endpoint**, que no está disponible en el plan Free.

Esta versión utiliza `ngrok tcp`, no `ngrok tls`.

Según la documentación actual de ngrok:
- Los endpoints TCP pueden utilizarse en el plan Free después de verificar un método de pago válido.
- ngrok indica que ese método de pago se utiliza para verificación y no se cobra por añadirlo.
- El host y el puerto TCP son asignados dinámicamente.
- El plan Free tiene límites de uso, entre ellos 1 GB/mes de transferencia saliente y 5.000 conexiones TCP/mes.

No se requiere contratar Pay-as-you-go ni reservar una dirección TCP de pago. Consulta las condiciones vigentes:
- https://ngrok.com/docs/gateway/endpoints/tcp
- https://ngrok.com/docs/pricing-limits
- https://ngrok.com/faq

Si tu cuenta no tiene un método de pago verificado, ngrok no permitirá iniciar el endpoint TCP gratuito. No existe una forma de evitar esa restricción con el comando `ngrok tcp`.

## 2. Advertencia: modo de laboratorio sin seguridad

Para evitar TLS, SASL y configuración adicional, el perfil aula utiliza Kafka con `PLAINTEXT` sin autenticación ni cifrado.

Esto significa que cualquier persona que conozca el endpoint público puede intentar conectarse al broker y leer o publicar mensajes en los topics. No utilices información privada, credenciales, datos personales ni cargas de trabajo de producción. Utiliza este modo únicamente para una demostración temporal y detén el túnel al terminar la clase.

El listener externo de Kafka está ligado a `127.0.0.1` dentro del Codespace; no se publica directamente el puerto 19092 en la interfaz de red del Codespace. ngrok es el único punto de entrada público.

## 3. Instalar ngrok en el Codespace

Instala el agente según la guía oficial para Debian/Ubuntu:

```bash
curl -sSL https://ngrok-agent.s3.amazonaws.com/ngrok.asc \
  | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null

echo "deb https://ngrok-agent.s3.amazonaws.com bookworm main" \
  | sudo tee /etc/apt/sources.list.d/ngrok.list

sudo apt update
sudo apt install ngrok
```

Agrega el authtoken de tu cuenta:

```bash
ngrok config add-authtoken TU_AUTHTOKEN
```

No publiques el authtoken en GitHub.

## 4. Configurar el perfil CLASSROOM

Crea el archivo local de configuración:

```bash
cp .env.classroom.example .env.classroom
```

Su contenido es:

```dotenv
KAFKA_PROFILE=CLASSROOM
KAFKA_TOPIC=etl.records
```

El archivo `.env.classroom` está ignorado por Git. No es necesario configurar un dominio: ngrok asignará un host y puerto TCP al iniciar.

Si Kafka ya está ejecutándose en modo LOCAL, detenlo antes de cambiar de perfil:

```bash
./stop_kafka.sh
```

## 5. Arrancar todo el entorno aula con un único comando

Con el perfil configurado en el paso anterior y Kafka local detenido, ejecuta en una sola terminal:

```bash
./start_ngrok_classroom.sh
```

El script realiza automáticamente estos pasos:

1. Inicia `ngrok tcp 19092`.
2. Captura el host y puerto TCP asignados por ngrok mediante su API local.
3. Guarda el endpoint en `.kafka/ngrok_tcp_address`.
4. Arranca Kafka en KRaft con el listener externo `127.0.0.1:19092`.
5. Configura `advertised.listeners` con la dirección pública correcta.
6. Mantiene ngrok activo mientras dure la sesión.

La salida será similar a:

```text
1/3 Iniciando ngrok TCP hacia 127.0.0.1:19092...
2/3 Esperando la dirección TCP asignada por ngrok...
Endpoint TCP: 1.tcp.ngrok.io:23456
3/3 Iniciando Kafka en modo CLASSROOM...

ENTORNO DE AULA LISTO
Kafka local del docente: localhost:9092
Endpoint TCP para estudiantes: 1.tcp.ngrok.io:23456
Topic: etl.records
Seguridad: PLAINTEXT sin autenticación ni cifrado.
```

Deja esta terminal abierta durante la clase. Si presionas `Ctrl+C`, el script detiene Kafka y ngrok.

Kafka queda configurado con estos listeners:

| Listener | Dirección | Función |
|---|---|---|
| INTERNAL | `127.0.0.1:9092` | Producer y administración del docente |
| EXTERNAL | `127.0.0.1:19092` | Conexiones que llegan desde ngrok |
| CONTROLLER | `127.0.0.1:9093` | Quorum KRaft |

Kafka anunciará la dirección pública que ngrok asignó en esa ejecución, por ejemplo:

```properties
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://1.tcp.ngrok.io:23456
```

El Producer del docente conserva `localhost:9092`; no requiere cambiar su código.

## 7. Obtener la dirección para los estudiantes

En el Codespace:

```bash
cat .kafka/ngrok_tcp_address
```

Comparte el resultado con los estudiantes. Por ejemplo:

```text
1.tcp.ngrok.io:23456
```

No compartas `localhost:9092`: esa dirección corresponde al entorno del docente.

## 8. Consumer desde Google Colab

Los estudiantes instalan únicamente el cliente:

```python
!pip -q install confluent-kafka
```

Configuran su conexión usando el host y puerto que tú compartiste:

```python
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "1.tcp.ngrok.io:23456"

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
print("Brokers:", {
    broker_id: f"{broker.host}:{broker.port}"
    for broker_id, broker in metadata.brokers.items()
})

consumer.subscribe(["etl.records"])
print("Suscrito a etl.records")
```

Después pueden utilizar el ciclo normal de consumo:

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

También se incluye el archivo `examples/classroom_consumer.py`.

## 9. Cada estudiante debe tener un group.id diferente

Para que cada estudiante reciba independientemente todos los mensajes, deben utilizar grupos distintos:

- `student-ana`
- `student-carlos`
- `student-03`

Si todos utilizan el mismo `group.id`, Kafka distribuye las particiones y los mensajes entre los integrantes del grupo.

Con `auto.offset.reset=latest`, un grupo nuevo comienza a recibir los mensajes publicados después de conectarse. Para leer los mensajes existentes, pueden usar `earliest` si el grupo aún no tiene offsets confirmados.

## 10. Enviar eventos desde el Producer del docente

El Producer continúa ejecutándose localmente:

```bash
source .venv/bin/activate
python producer.py
```

En otra terminal:

```bash
python insert_record.py --station ST-01 --temperature 27.4 --humidity 71.2 --status normal
```

El evento se publica en `etl.records` y todos los grupos de consumidores independientes podrán recibirlo.

## 11. Diagnóstico

En el Codespace:

```bash
./diagnose_kafka.sh
```

Verifica los listeners:

```bash
grep -E '^(listeners|advertised.listeners|inter.broker.listener.name|listener.security.protocol.map)=' .kafka/server.properties
```

Verifica el listener local de ngrok:

```bash
ss -lnt | grep 19092
```

Si el consumer logra conectarse al bootstrap pero después falla al pedir metadata, revisa que:
- Kafka anuncie exactamente el host y puerto de ngrok.
- El proceso ngrok siga activo.
- El endpoint público no haya cambiado.
- El broker esté escuchando en 127.0.0.1:19092.

## 12. Detener el laboratorio

En la terminal donde ejecutaste `./start_ngrok_classroom.sh`, presiona `Ctrl+C`. El script detiene Kafka y ngrok y elimina el endpoint local guardado.

El endpoint TCP dinámico deja de estar disponible. En la siguiente sesión ngrok podría asignar otra dirección; basta con volver a ejecutar `./start_ngrok_classroom.sh` para iniciar todo el entorno y obtener la nueva dirección.

## Límites del laboratorio

- Un único broker KRaft, tres particiones y factor de replicación 1. No hay alta disponibilidad.
- El listener externo es público y no requiere autenticación ni cifrado.
- Cualquier cliente que conozca la dirección puede intentar leer o publicar mensajes.
- ngrok Free requiere método de pago verificado para endpoints TCP y aplica límites de transferencia y conexiones.
- El endpoint TCP es dinámico; Kafka debe reiniciarse si cambia la dirección anunciada.
- No utilices esta configuración en producción ni con datos sensibles.

## Preguntas de discusión

1. ¿Qué diferencia existe entre `listeners` y `advertised.listeners`?
2. ¿Por qué el endpoint ngrok debe iniciarse antes que Kafka en este laboratorio?
3. ¿Qué ocurre si ngrok asigna un puerto diferente al reiniciar?
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

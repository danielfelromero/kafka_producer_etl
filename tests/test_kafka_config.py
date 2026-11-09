"""Regression tests for LOCAL and CLASSROOM Kafka broker configs."""

from pathlib import Path
import tempfile
import unittest

from configure_kafka import generate_config


TEMPLATE = """process.roles=broker,controller
node.id=1
controller.quorum.voters=old
controller.listener.names=OLD
log.dirs=/old/path
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://old:9092
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
sasl.enabled.mechanisms=PLAIN
listener.name.external.plain.sasl.jaas.config=old
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:old
allow.everyone.if.no.acl.found=false
num.partitions=3
"""


class KafkaConfigTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.template = self.root / "server.template"
        self.template.write_text(TEMPLATE, encoding="utf-8")
        self.output = self.root / ".kafka" / "server.properties"
        self.data_dir = self.root / ".kafka" / "data"

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def properties(content):
        result = {}
        for line in content.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key, value = stripped.split("=", 1)
                result[key.strip()] = value.strip()
        return result

    def test_local_profile_preserves_existing_local_contract(self):
        content = generate_config(
            self.template, self.output, self.data_dir, "LOCAL"
        )
        props = self.properties(content)

        self.assertEqual(
            props["listeners"],
            "PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
        )
        self.assertEqual(
            props["advertised.listeners"], "PLAINTEXT://localhost:9092"
        )
        self.assertEqual(props["inter.broker.listener.name"], "PLAINTEXT")
        self.assertEqual(props["process.roles"], "broker,controller")
        self.assertEqual(props["node.id"], "1")
        self.assertEqual(props["num.partitions"], "3")
        self.assertNotIn("sasl.enabled.mechanisms", props)
        self.assertNotIn("authorizer.class.name", props)
        self.assertNotIn("super.users", props)

    def test_classroom_profile_announces_bore_endpoint(self):
        content = generate_config(
            self.template,
            self.output,
            self.data_dir,
            "CLASSROOM",
            "bore.pub:54321",
        )
        props = self.properties(content)

        self.assertEqual(
            props["listeners"],
            "INTERNAL://127.0.0.1:9092,EXTERNAL://127.0.0.1:19092,"
            "CONTROLLER://127.0.0.1:9093",
        )
        self.assertEqual(
            props["advertised.listeners"],
            "INTERNAL://localhost:9092,EXTERNAL://bore.pub:54321",
        )
        self.assertEqual(props["inter.broker.listener.name"], "INTERNAL")
        self.assertIn("EXTERNAL:PLAINTEXT", props["listener.security.protocol.map"])
        self.assertEqual(props["process.roles"], "broker,controller")
        self.assertEqual(props["node.id"], "1")
        self.assertNotIn("sasl.enabled.mechanisms", props)
        self.assertNotIn("authorizer.class.name", props)
        self.assertNotIn("super.users", props)
        self.assertTrue(self.output.exists())
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o600)

    def test_classroom_rejects_invalid_public_endpoint(self):
        for value in ("localhost:9092", "bore.pub:0", "bore.pub:65536", ""):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    generate_config(
                        self.template,
                        self.output,
                        self.data_dir,
                        "CLASSROOM",
                        value,
                    )

    def test_invalid_profile_is_rejected(self):
        with self.assertRaises(ValueError):
            generate_config(
                self.template, self.output, self.data_dir, "UNKNOWN"
            )


if __name__ == "__main__":
    unittest.main()

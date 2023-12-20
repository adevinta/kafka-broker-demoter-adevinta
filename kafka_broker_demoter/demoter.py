import copy
import json
import logging
import os
import random
import string
import subprocess
import tempfile

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

from kafka_broker_demoter.exceptions import (
    BrokerStatusError,
    ChangeReplicaAssignmentError,
    PreferredLeaderMismatchCurrentLeader,
    TriggerLeaderElectionError,
)

logger = logging.getLogger(__name__)


class Demoter(object):
    TOPIC_TRACKER = "__demote_tracker"

    def __init__(
        self,
        bootstrap_servers="localhost:9092",
        kafka_path="/opt/kafka",
        kafka_heap_opts="-Xmx512M",
        topic_tracker=TOPIC_TRACKER,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.kafka_path = kafka_path
        self.kafka_heap_opts = kafka_heap_opts
        self.topic_tracker = topic_tracker

        self.admin_client = None

    @property
    def _get_admin_client(self):
        if self.admin_client is None:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
        return self.admin_client

    def _get_topics_metadata(self):
        topic_metadata = self._get_admin_client.describe_topics()
        return topic_metadata

    def _get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers, compression_type="lz4"
        )

    def _produce_record(self, key, value):
        serialized_key = str(key).encode("utf-8")
        serialized_value = json.dumps(value).encode("utf-8")
        producer = self._get_producer()
        producer.send(self.topic_tracker, key=serialized_key, value=serialized_value)
        producer.flush()
        producer.close()
        logger.info("Produced record with key {} and value {}".format(key, value))

    def _remove_non_existent_topics(self, broker_id):
        partitions = self._consume_latest_record_per_key(broker_id)
        if partitions is None:
            logger.debug("No partitions found for broker {}".format(broker_id))
            return None

        existing_topics = [topic["topic"] for topic in self._get_topics_metadata()]
        new_partitions = []

        for partition in partitions["partitions"]:
            topic = partition.get("topic", "")
            if topic in existing_topics:
                new_partitions.append(partition)
        return {"partitions": new_partitions}

    def _get_consumer(self):
        consumer = KafkaConsumer(
            self.topic_tracker,
            group_id=self.topic_tracker,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            bootstrap_servers=self.bootstrap_servers,
        )
        return consumer

    def _consume_latest_record_per_key(self, key):
        consumer = self._get_consumer()
        records = consumer.poll(timeout_ms=20000)
        latest_record = None
        for topic_partition, record_list in records.items():
            for record in record_list:
                if int(record.key.decode("utf-8")) == key:
                    latest_record = json.loads(record.value.decode("utf-8"))
        logger.debug("Latest record found for key {}: {}".format(key, latest_record))
        consumer.close()
        return latest_record

    def _get_partition_leaders_by_broker_id(self, broker_id):
        partitions = {"partitions": []}
        for topic in self._get_topics_metadata():
            topic_name = topic["topic"]
            for partition in topic["partitions"]:
                partition_id = partition["partition"]
                leader = partition["leader"]
                replicas = partition["replicas"]
                if broker_id == leader and len(replicas) > 1:
                    if leader != replicas[0]:
                        raise PreferredLeaderMismatchCurrentLeader
                    partitions["partitions"].append(
                        {
                            "topic": topic_name,
                            "partition": int(partition_id),
                            "replicas": [int(replica) for replica in replicas],
                        }
                    )
        return partitions

    def _get_demoting_proposal(self, broker_id, current_partitions_state):
        demoting_plan = copy.deepcopy(current_partitions_state)
        for counter, partition in enumerate(demoting_plan["partitions"]):
            replicas = partition["replicas"]
            reassigned_replicas = [replicas[-1]] + replicas[:-1]
            demoting_plan["partitions"][counter]["replicas"] = reassigned_replicas
        return demoting_plan

    def _create_topic(self):
        topics = self._get_admin_client.list_topics()
        if self.topic_tracker not in topics:
            logger.info(
                "Creating a new topic called {} for tracking broker demotion rollback".format(
                    self.topic_tracker
                )
            )
            topic = NewTopic(
                name=self.topic_tracker,
                num_partitions=1,
                replication_factor=3,
                topic_configs={"cleanup.policy": "compact"},
            )
            self._get_admin_client.create_topics(
                new_topics=[topic], validate_only=False
            )

    def demote(self, broker_id):
        self._create_topic()
        if self._consume_latest_record_per_key(broker_id) is not None:
            raise BrokerStatusError(
                "Ongoing or unfinished demote operation was found for broker {}".format(
                    broker_id
                )
            )
        current_partitions_state = self._get_partition_leaders_by_broker_id(broker_id)
        if not current_partitions_state["partitions"]:
            logger.info(
                "Broker {} already demoted, no partition leaders found".format(
                    broker_id
                )
            )
            return None
        else:
            demoted_partitions_state = self._get_demoting_proposal(
                broker_id, current_partitions_state
            )
            self._change_replica_assignment(demoted_partitions_state)
            self._trigger_leader_election(demoted_partitions_state)
            self._save_rollback_plan(broker_id, current_partitions_state)

    def demote_rollback(self, broker_id):
        previous_partitions_state = self._remove_non_existent_topics(broker_id)
        if previous_partitions_state is None:
            raise BrokerStatusError(
                "Previous demote operation on broker {} was not found, there is nothing to rollback".format(
                    broker_id
                )
            )
        self._change_replica_assignment(previous_partitions_state)
        self._trigger_leader_election(previous_partitions_state)
        self._produce_record(broker_id, None)
        logger.info(
            "Rollback plan for broker {} was successfully executed".format(broker_id)
        )

    def _generate_tempfile_with_json_content(self, data):
        filename = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
        temp_filepath = tempfile.mktemp(suffix=".json", prefix=filename)
        with open(temp_filepath, "w") as temp_file:
            json.dump(data, temp_file)

        return temp_filepath

    def _change_replica_assignment(self, demoting_plan):
        demoting_plan_filepath = self._generate_tempfile_with_json_content(
            demoting_plan
        )
        command = "{}/bin/kafka-reassign-partitions.sh --bootstrap-server {} --reassignment-json-file {} --execute --timeout 60".format(
            self.kafka_path, self.bootstrap_servers, demoting_plan_filepath
        )
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, env=env_vars
        )

        if result.returncode != 0:
            raise ChangeReplicaAssignmentError(result.stdout.strip())

    def _generate_tmpfile_with_admin_configs(self):
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        tmp_file.write(
            "default.api.timeout.ms=240000\nrequest.timeout.ms=120000".encode()
        )
        tmp_file.close()
        return tmp_file.name

    def _trigger_leader_election(self, demoting_plan):
        demoting_plan_filepath = self._generate_tempfile_with_json_content(
            demoting_plan
        )
        command = "{}/bin/kafka-leader-election.sh --admin.config {} --bootstrap-server {} --election-type PREFERRED --path-to-json-file {}".format(
            self.kafka_path,
            self._generate_tmpfile_with_admin_configs(),
            self.bootstrap_servers,
            demoting_plan_filepath,
        )
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, env=env_vars
        )

        if result.returncode != 0:
            logger.error(
                "Failed to trigger leader election, error: {}, command: {}".format(
                    result.stdout.strip(), command
                )
            )
            raise TriggerLeaderElectionError(result.stdout.strip())

    def _save_rollback_plan(self, broker_id, current_partitions_state):
        logger.info("Saving rollback plan for broker {}".format(broker_id))
        self._produce_record(broker_id, current_partitions_state)

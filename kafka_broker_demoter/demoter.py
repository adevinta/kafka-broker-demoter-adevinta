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
from tenacity import retry, stop_after_delay, wait_fixed

from kafka_broker_demoter.exceptions import (
    BrokerStatusError,
    ChangeReplicaAssignmentError,
    PreferredLeaderMismatchCurrentLeader,
    ProduceRecordError,
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
        self.partitions_temp_filepath = None
        self.admin_config_tmp_file = None
        self.admin_config_content = (
            "default.api.timeout.ms=240000\nrequest.timeout.ms=120000"
        )

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
            bootstrap_servers=self.bootstrap_servers,
            compression_type="lz4",
            retries=3,
        )

    @retry(stop=stop_after_delay(6), wait=wait_fixed(1), reraise=True)
    def _produce_record(self, key, value):
        serialized_key = str(key).encode("utf-8")
        serialized_value = json.dumps(value).encode("utf-8")
        try:
            producer = self._get_producer()
            future = producer.send(
                self.topic_tracker, key=serialized_key, value=serialized_value
            )
            future.get(timeout=10)
        except Exception as e:
            logger.error("Failed to produce message: {}, trying again...".format(e))
            raise ProduceRecordError

        logger.info(
            "Successful produced record with key {} and value {}".format(key, value)
        )

    def _remove_non_existent_topics(self, broker_id):
        """
        Removes non-existent topics from the list of partitions.

        Args:
            broker_id: The ID of the broker.

        Returns:
            A list of partitions that correspond to existing topics.

        Raises:
            None.

        Note:
            This method first retrieves the latest record of partition information for the given broker_id using the
            `_consume_latest_record_per_key` method. Then, it checks if any partitions are found. If not, it returns None.
            Otherwise, it retrieves the list of existing topics using the `_get_topics_metadata` method. Finally, it
            extracts the partitions that correspond to existing topics and returns them as a new list.

        """
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
        """
        Retrieves the latest record for a given key from the consumer.

        Args:
            key: The key to filter the records.

        Returns:
            The latest record (deserialized JSON object) found for the given key.

        Raises:
            ValueError: If the record's key cannot be decoded as UTF-8 or parsed as an integer.

        Note:
            This method assumes that the consumer has already been configured and initialized.

        """
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
        """
        Retrieves partition leaders by broker ID.

        Args:
            broker_id: The ID of the broker.

        Returns:
            A dictionary containing the partitions with their leaders.

        Raises:
            PreferredLeaderMismatchCurrentLeader: If the leader and the first replica of a partition do not match.

        Note:
            This method iterates over the topics obtained from the `_get_topics_metadata` method. For each topic,
            it retrieves the topic name, partition ID, leader, and replicas. If the leader matches the specified
            broker ID and there are more than one replica, it checks if the leader and the first replica match.
            If not, it raises the `PreferredLeaderMismatchCurrentLeader` exception. Finally, it appends the partition
            information to the `partitions` list. The method returns a dictionary containing the partitions and their leaders.

        """
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
        """
        Generates a proposal for demoting a broker.

        Args:
            broker_id: The ID of the broker to be demoted.
            current_partitions_state: The current state of partitions.

        Returns:
            A new partition state with the broker demotion proposal.

        Note:
            This method produces a new partition state by creating a deep copy of the current partitions state.
            It then iterates through each partition and rearranges the replicas by moving the last replica to the
            beginning of the list and shifting the remaining replicas to the right. This effectively demotes the
            specified broker's replica to the last position. The method returns the updated partition state.

        """
        demoting_plan = copy.deepcopy(current_partitions_state)
        for counter, partition in enumerate(demoting_plan["partitions"]):
            replicas = partition["replicas"]
            reassigned_replicas = [replicas[-1]] + replicas[:-1]
            demoting_plan["partitions"][counter]["replicas"] = reassigned_replicas
        return demoting_plan

    def _create_topic(self):
        """
        Creates a new topic for tracking broker demotion rollback.

        Returns:
            None

        Note:
            This method checks if the topic specified in `self.topic_tracker` exists in the Kafka cluster.
            If the topic does not exist, it creates a new topic with the specified name, number of partitions,
            replication factor, and topic configuration. The creation of the topic is done through the admin client
            obtained from `_get_admin_client` method.

        """
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
        """
        Demotes a broker by reassigning partition leaders and triggering leader election.

        Args:
            broker_id: The ID of the broker to be demoted.

        Returns:
            None

        Raises:
            BrokerStatusError: If an ongoing or unfinished demote operation is found for the specified broker.

        Note:
            This method performs the following steps to demote a broker:
            1. Calls the `_create_topic` method to create a topic for tracking broker demotion rollback.
            2. Checks if there is an ongoing or unfinished demote operation for the specified broker by calling
               the `_consume_latest_record_per_key` method. If such an operation exists, it raises a `BrokerStatusError`.
            3. Retrieves the current partition state (partition leaders) for the specified broker by calling
               the `_get_partition_leaders_by_broker_id` method.
            4. If there are no partition leaders for the broker, it logs a message and returns.
            5. Otherwise, it generates the proposed state of demoted partitions using the `_get_demoting_proposal` method.
            6. Calls the `_change_replica_assignment` method to reassign replica assignments according to the proposed state.
            7. Triggers leader election for the demoted partitions using the `_trigger_leader_election` method.
            8. Saves the rollback plan for the demotion operation by calling the `_save_rollback_plan` method.

        """
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
        """
        Rolls back the demotion of a broker.

        Args:
            broker_id: The ID of the broker to roll back the demotion for.

        Returns:
            None

        Raises:
            BrokerStatusError: If the previous demotion operation for the specified broker is not found.

        Note:
            This method performs the following steps to roll back the demotion of a broker:
            1. Calls the `_remove_non_existent_topics` method to remove any topic that was deleted during the demotion process as an non existen topic in hte json list will make the process to fail
            2. If the previous partition state is not found, it raises a `BrokerStatusError`.
            3. Calls the `_change_replica_assignment` method to reassign replica assignments according to the previous state.
            4. Triggers leader election for the restored partitions using the `_trigger_leader_election` method.
            5. Calls the `_produce_record` method to produce a record, indicating the rollback operation was executed.

        """
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
        """
        Generates a temporary file with the specified JSON content.

        Args:
            data (dict): A dictionary representing the JSON content.

        Returns:
            str: The filepath of the generated temporary file.

        Raises:
            None.

        Notes:
            - If `self.partitions_temp_filepath` is already set, a new temporary file will not be generated.
            - The generated filepath will have a random filename consisting of lowercase letters and digits.
            - The generated filepath will have the '.json' extension.

        """
        if self.partitions_temp_filepath is None:
            filename = "".join(
                random.choices(string.ascii_lowercase + string.digits, k=10)
            )
            self.partitions_temp_filepath = tempfile.mktemp(
                suffix=".json", prefix=filename
            )

        with open(self.partitions_temp_filepath, "w") as temp_file:
            json.dump(data, temp_file)
            return self.partitions_temp_filepath

    def _change_replica_assignment(self, demoting_plan):
        """
        Changes the replica assignment according to a demoting plan.

        Args:
            demoting_plan: A dictionary representing the demoting plan, which contains the new replica assignments for
                           each topic and partition.

        Raises:
            ChangeReplicaAssignmentError: If there is an error executing the change replica assignment command.

        Note:
            This method performs the following steps:
            1. Generates a temporary file with the demoting plan contents using the `_generate_tempfile_with_json_content` method.
            2. Constructs the command to execute the `kafka-reassign-partitions.sh` script with the specified options.
            3. Sets the `KAFKA_HEAP_OPTS` environment variable to the `kafka_heap_opts` value.
            4. Executes the command using the `subprocess.run` function, capturing the output and setting it as text.
            5. If the return code of the command is not 0, it raises a `ChangeReplicaAssignmentError` with the stripped
              output as the error message.

        """
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
        """
        Generates a temporary file with the admin configurations.

        Returns:
            str: The name of the temporary file.

        Note:
            This method performs the following steps:
            1. If the `admin_config_tmp_file` attribute is `None`, it creates a new temporary file using the
              `tempfile.NamedTemporaryFile` function and assigns it to the `admin_config_tmp_file` attribute.
            2. Writes the admin_config_content to the temporary file as bytes.
            3. Closes the temporary file.
            4. Returns the name of the temporary file.

        """
        if self.admin_config_tmp_file is None:
            self.admin_config_tmp_file = tempfile.NamedTemporaryFile(delete=False)

        self.admin_config_tmp_file.write(self.admin_config_content.encode())
        self.admin_config_tmp_file.close()
        return self.admin_config_tmp_file.name

    def _trigger_leader_election(self, demoting_plan):
        """
        Triggers leader election for a demoting plan.

        Args:
            demoting_plan (dict): The demoting plan specifying the partition and its new leader.

        Raises:
            TriggerLeaderElectionError: If the leader election fails.

        Returns:
            None

        Note:
            This method performs the following steps:
            1. Generates a temporary file using the `_generate_tempfile_with_json_content` method and assigns
              the file path to `demoting_plan_filepath`.
            2. Constructs the leader election command using the provided Kafka path, admin configs file,
              bootstrap servers, and demoting plan file path.
            3. Sets the `KAFKA_HEAP_OPTS` environment variable to `self.kafka_heap_opts`.
            4. Executes the leader election command using `subprocess.run`.
            5. If the return code is not 0, logs an error and raises a `TriggerLeaderElectionError`.
        """
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

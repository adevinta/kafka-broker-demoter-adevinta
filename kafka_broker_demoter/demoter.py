import copy
import json
import logging
import os
import random
import re
import string
import subprocess
import tempfile
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from kafka_broker_demoter.exceptions import (
    BrokerStatusError,
    ProduceRecordError,
    RecordNotFoundError,
    SetTopicThrottleError,
    SubprocessExecutionError,
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
        self.admin_config_content = (
            "default.api.timeout.ms=240000\nrequest.timeout.ms=120000"
        )
        self.admin_config_tmp_file = self._generate_tmpfile_with_admin_configs()

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

    @retry(
        stop=stop_after_attempt(6),
        wait=wait_fixed(1),
        reraise=True,
        retry=retry_if_exception_type(ProduceRecordError),
    )
    def _produce_record(self, key, value):
        """
        Retry producing a record with given key and value to a topic.

        Args:
            key: The key of the record.
            value: The value of the record.

        Raises:
            ProduceRecordError: If the record fails to be produced after retries.
        """
        serialized_key = str(key).encode("utf-8")
        serialized_value = json.dumps(value).encode("utf-8")
        try:
            producer = self._get_producer()
            future = producer.send(
                self.topic_tracker, key=serialized_key, value=serialized_value
            )
            future.get(timeout=10)
        except Exception as e:
            logger.warning("Failed to produce message: {}, trying again...".format(e))
            raise ProduceRecordError

        # Make sure record was saved
        self._consume_latest_record_per_key(key)

        logger.debug(
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
            logger.info("No partitions found for broker {}".format(broker_id))
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

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_fixed(1),
        reraise=True,
        retry=retry_if_exception_type(RecordNotFoundError),
    )
    def _consume_latest_record_per_key(self, key):
        """
        Consume and retrieve the latest record for a given key.

        Args:
            key: The key used to filter the records.

        Returns:
            dict: The latest record payload for the given key.

        Raises:
            RecordNotFoundError: If no record is found for the given key.
        """
        consumer = self._get_consumer()
        latest_record_payload = {}

        while True:
            records = consumer.poll(timeout_ms=5000, max_records=100)
            if len(records) == 0:
                break
            for topic_partition, record_list in records.items():
                for record in record_list:
                    if int(record.key.decode("utf-8")) == key:
                        latest_record_payload[key] = json.loads(
                            record.value.decode("utf-8")
                        )
        consumer.close()

        if len(latest_record_payload) == 0:
            logger.warning("Latest record not found for key {}".format(key))
            raise RecordNotFoundError
        logger.debug(
            "Latest record found for key {}: {}".format(key, latest_record_payload[key])
        )
        return latest_record_payload[key]

    def _get_partition_leaders_by_broker_id(self, broker_id):
        """
        Retrieves partition leader information for a specified broker ID.

        Args:
            broker_id (int): The ID of the broker to fetch partition leaders.

        Returns:
            dict: A dictionary with partition information for the specified broker as leader.

        This method retrieves partition leader information for a specific broker in a Kafka cluster.
        It filters the topics and partitions in the cluster, storing relevant information (topic name, partition ID, and replica IDs)
        only for partitions where the specified broker is the leader.

        Note: This method relies on other helper methods, such as `_get_topics_metadata()`, to fetch the necessary metadata.

        """
        partitions = {"partitions": []}
        for topic in self._get_topics_metadata():
            topic_name = topic["topic"]
            for partition in topic["partitions"]:
                partition_id = partition["partition"]
                leader = partition["leader"]
                replicas = partition["replicas"]
                if broker_id == leader and len(replicas) > 1:
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
        Get a demoting proposal for the specified broker by reassigning replicas.

        Args:
            broker_id: The ID of the broker to generate the demoting proposal for.
            current_partitions_state: The current state of the partitions.

        Returns:
            dict: A demoting proposal with reassigned replicas.

        """
        demoting_plan = copy.deepcopy(current_partitions_state)
        for counter, partition in enumerate(demoting_plan["partitions"]):
            replicas = partition["replicas"]
            reassigned_replicas = [replicas[-1]] + replicas[:-1]
            demoting_plan["partitions"][counter]["replicas"] = reassigned_replicas
        return demoting_plan

    def _create_topic(self):
        """
        Create a new topic for tracking broker demotion rollback, if it doesn't already exist.

        Returns:
            None

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

    def demote(self, broker_id, throttle, num_concurrent_leader_movements):
        """
        Demotes a broker by reassigning the partition leaders from the specified broker to other brokers.
        If an ongoing or unfinished demote operation is found for the broker, a BrokerStatusError is raised.
        If a RecordNotFoundError occurs while checking for ongoing demote operations, the broker is considered ready for demotion.

        Args:
            broker_id (int): The ID of the broker to be demoted.

        Returns:
            None: If the broker is already demoted and no partition leaders are found.
        """
        self._create_topic()
        try:
            if self._consume_latest_record_per_key(broker_id) is not None:
                raise BrokerStatusError(
                    "Ongoing or unfinished demote operation was found for broker {}".format(
                        broker_id
                    )
                )
        except RecordNotFoundError:
            logger.info(
                "Ongoing or unfinished demote operation was not found for broker {}, proceeding to demote the broker".format(
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
            logger.info(
                "Moving leaderships away from broker: {}, it may take a while...".format(
                    broker_id
                )
            )
            self._change_replica_assignment(demoted_partitions_state)
            self._trigger_leader_election(demoted_partitions_state, num_concurrent_leader_movements)

            self._save_rollback_plan(broker_id, current_partitions_state)
            if throttle > 0:
                self._set_throttles(broker_id, throttle)

        logger.info("Broker {} was successfully demoted".format(broker_id))

    def _unset_throttles(self, broker_id):
        """
        Unsets the throttles for previously set for a borker demotion.

        Args:
            broker_id (int): The ID to track the previous broker demtion.

        Returns:
            None

        Raises:
            None
        """
        follower_replicas_to_remove_throttle = (
            self._get_follower_partitions_of_demoted_broker(broker_id)
        )
        leader_replicas_to_remove_throttle = (
            self._get_leader_partitions_of_replicas_belonging_to_demoted_broker(
                broker_id
            )
        )
        self._unset_brokers_throttle(broker_id)
        self._unset_partitions_throttle(
            "follower", follower_replicas_to_remove_throttle
        )
        self._unset_partitions_throttle("leader", leader_replicas_to_remove_throttle)

    def _execute_subprocess(self, command, env_vars, raise_exception_on_failure=True):
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, env=env_vars
        )

        if result.returncode != 0 and raise_exception_on_failure:
            logger.error(
                "Failed to execute subprocess command: {}, error: {}".format(
                    command,
                    result.stderr.strip(),
                )
            )
            raise SubprocessExecutionError(result.stderr.strip())
        else:
            return result

    def _unset_brokers_throttle(self, demoted_broker_id):
        """
        Remove the replication throttle on all brokers in the Kafka cluster.

        Args:
            throttle (int): The replication throttle rate in bytes per second.

        Raises:
            SetBrokerThrottleError: If an error occurs while setting the throttle on a broker.

        Returns:
            None
        """
        cluster_metadata = self._get_admin_client.describe_cluster()
        broker_ids = [broker["node_id"] for broker in cluster_metadata["brokers"]]
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        raw_command = "{}/bin/kafka-configs.sh --bootstrap-server {} --alter --delete-config {}.replication.throttled.rate --entity-type brokers --entity-name {}"

        for broker_id in broker_ids:
            if broker_id == demoted_broker_id:
                command = raw_command.format(
                    self.kafka_path, self.bootstrap_servers, "follower", broker_id
                )
                self._execute_subprocess(command, env_vars)
            else:
                command = raw_command.format(
                    self.kafka_path, self.bootstrap_servers, "leader", broker_id
                )
                self._execute_subprocess(command, env_vars)
        logger.info("Throttle has been removed in all the brokers")

    def _set_throttles(self, demoted_broker_id, throttle):
        """
        Sets the replication throttles on all brokers and topcis in the Kafka cluster.

        Args:
            demoted_broker_id (int): The demoted broker id to create the map of partitions to be throttled.
            throttle (int): The replication throttle rate in bytes per second.

        Raises:
            None

        Returns:
            None
        """
        follower_replicas_to_throttle = self._get_follower_partitions_of_demoted_broker(
            demoted_broker_id
        )
        leader_replicas_to_throttle = (
            self._get_leader_partitions_of_replicas_belonging_to_demoted_broker(
                demoted_broker_id
            )
        )

        self._set_brokers_to_be_throttled(throttle, demoted_broker_id)
        self._set_partitions_to_be_throttled("follower", follower_replicas_to_throttle)
        self._set_partitions_to_be_throttled("leader", leader_replicas_to_throttle)

    def _set_brokers_to_be_throttled(self, throttle, demoted_broker_id, broker_ids=[]):
        """
        Sets the replication throttle on all brokers in the Kafka cluster.

        Args:
            throttle (int): The replication throttle rate in bytes per second.

        Raises:
            SetBrokerThrottleError: If an error occurs while setting the throttle on a broker.

        Returns:
            None
        """
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        cluster_metadata = self._get_admin_client.describe_cluster()
        broker_ids_to_throttle = None
        raw_command = "{}/bin/kafka-configs.sh --bootstrap-server {} --alter --add-config {}.replication.throttled.rate={} --entity-type brokers --entity-name {}"
        if len(broker_ids) == 0:
            broker_ids_to_throttle = [
                broker["node_id"] for broker in cluster_metadata["brokers"]
            ]
        else:
            broker_ids_to_throttle = broker_ids

        for broker_id in broker_ids_to_throttle:
            if broker_id == demoted_broker_id:
                command = raw_command.format(
                    self.kafka_path,
                    self.bootstrap_servers,
                    "follower",
                    throttle,
                    broker_id,
                )
            else:
                command = raw_command.format(
                    self.kafka_path,
                    self.bootstrap_servers,
                    "leader",
                    throttle,
                    broker_id,
                )
            self._execute_subprocess(command, env_vars)

        logger.info(
            "Throttle of {} bytes/sec has been applied in brokers: {}".format(
                throttle, broker_ids_to_throttle
            )
        )

    def _unset_partitions_throttle(self, type, replicas_to_remove_throttle):
        """
        Unsets the throttle for specific partitions on topics.

        Args:
            type (str): Throttle type to be removed (e.g., 'follower', 'leader').
            replicas_to_remove_throttle (dict): Dictionary containing partition IDs and broker IDs for which the throttle
                                            needs to be removed.

        Returns:
            None

        Raises:
            SetTopicThrottleError: If the throttle removal fails for any partition.
        """
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts

        for topic, replicas in replicas_to_remove_throttle.items():
            command = "{}/bin/kafka-configs.sh --bootstrap-server {} --alter --delete-config {}.replication.throttled.replicas --entity-type topics --entity-name {}".format(
                self.kafka_path,
                self.bootstrap_servers,
                type,
                topic,
            )
            result = self._execute_subprocess(
                command, env_vars, raise_exception_on_failure=False
            )
            if result.returncode != 0:
                # If the topic was recreated the throttle does not exist anymore, so we can skip the error when trying to remove the throttle as it does not exist
                if (
                    "Invalid config(s): {}.replication.throttled.replicas".format(type)
                    in result.stderr
                ):
                    logger.warning(
                        "Throttle type: {} on topic {} could not be deleted as it does not exist, maybe the topic was recreated duirng the demotion process. command: {}".format(
                            type, topic, command
                        )
                    )
                    continue

                logger.error(
                    "Failed to trigger set throttle type {} on topic {} , error: {}, command: {}".format(
                        type, topic, result.stderr.strip(), command
                    )
                )
                raise SetTopicThrottleError(result.stderr.strip())

            logger.info(
                "{} throttle on topic {} has been removed on specific partitions (partitonId:brokerId) {}".format(
                    type, topic, replicas
                )
            )

    def _set_partitions_to_be_throttled(self, type, replicas_to_throttle):
        """
        Sets the throttle for specific partitions on topics.

        Args:
            type (str): Throttle type to be applied (e.g., 'follower', 'leader').
            replicas_to_throttle (dict): Dictionary containing partition IDs and broker IDs for which the throttle
                                     needs to be applied.

        Returns:
            None

        Raises:
            SetTopicThrottleError: If the throttle application fails for any partition.
        """
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        raw_command = "{}/bin/kafka-configs.sh --bootstrap-server {} --alter --add-config {}.replication.throttled.replicas=[{}] --entity-type topics --entity-name {}"

        for topic, replicas in replicas_to_throttle.items():
            command = raw_command.format(
                self.kafka_path,
                self.bootstrap_servers,
                type,
                replicas,
                topic,
            )
            self._execute_subprocess(command, env_vars)

            logger.info(
                "{} throttle on topic {} has been applied on specific partitions (partitonId:brokerId) {}".format(
                    type, topic, replicas
                )
            )

    def _get_follower_partitions_of_demoted_broker(self, broker_id):
        """
        Gets a dictionary where the key is the topic name and the value is a comma-separated string of partition:broker_id
        pairs for partitions that have the given broker ID as one of their replicas.

        Args:
            broker_id (int): The ID of the broker.

        Returns:
            dict: A dictionary where the key is the topic name and the value is a comma-separated string of partition:broker_id
            pairs for partitions with the given broker ID as one of their replicas.

        Example:
            _get_follower_partitions_of_demoted_broker(3)
            Returns:
            {'topic1': '0:3,1:3,2:3', 'topic2': '1:3', 'topic3': '3:3,4:3'}
        """
        topic_metadata = self._get_topics_metadata()
        topics = {}

        for topic in topic_metadata:
            topic_name = topic["topic"]
            for partition in topic["partitions"]:
                partition_id = partition["partition"]
                replicas = partition["replicas"]
                if int(broker_id) in replicas:
                    if topic_name not in topics:
                        topics[topic_name] = []
                    topics[topic_name].append("{}:{}".format(partition_id, broker_id))

        result = {}
        for name, partitions in topics.items():
            result[name] = ",".join(partitions)
        return result

    def _get_leader_partitions_of_replicas_belonging_to_demoted_broker(self, broker_id):
        """
        Gets a dictionary where the key is the topic name and the value is a comma-separated string of partition:leader_broker_id
        pairs for partitions that have the given broker ID as one of their replicas.

        Args:
            broker_id (int): The ID of the broker.

        Returns:
            dict: A dictionary where the key is the topic name and the value is a comma-separated string of partition:leader_broker_id
            pairs for partitions with the given broker ID as one of their replicas.

        Example:
            _get_leader_partitions_of_replicas_belonging_to_demoted_broker(3)
            Returns:
            {'topic1': '0:2,1:3,2:1', 'topic2': '1:2', 'topic3': '3:3,4:2'}
        """
        topic_metadata = self._get_admin_client.describe_topics()

        topics = {}
        for topic in topic_metadata:
            topic_name = topic["topic"]
            for partition in topic["partitions"]:
                partition_id = partition["partition"]
                replicas = partition["replicas"]
                leader_broker_id = partition["leader"]
                if int(broker_id) in replicas:
                    if topic_name not in topics:
                        topics[topic_name] = []
                    topics[topic_name].append(
                        "{}:{}".format(partition_id, leader_broker_id)
                    )

        result = {}
        for name, partitions in topics.items():
            result[name] = ",".join(partitions)
        return result

    def update_throttle(self, demoted_broker_id, throttle, broker_ids=[]):
        """
        Update throttle value for broker demotion.

        Parameters:
        - demoted_broker_id (int): The ID of the demoted broker.
        - throttle (int or float): The throttle value to be set.
        - broker_ids (list): Optional list of broker IDs to be throttled.

        Returns:
        - None

        Raises:
        - RecordNotFoundError: If there is no ongoing or unfinished demotion operation for the given broker.
        """
        # self._consume_latest_record_per_key() can find a Null record for a previous demotion or raise RecordNotFoundError when the record does not exist
        # We only update throttles when a broker is demoted
        try:
            if self._consume_latest_record_per_key(demoted_broker_id) is None:
                raise RecordNotFoundError
        except RecordNotFoundError:
            logger.error(
                "Ongoing or unfinished demote operation was not found for broker {}, Can't update the throttle value".format(
                    demoted_broker_id
                )
            )
            raise
        self._set_brokers_to_be_throttled(throttle, demoted_broker_id, broker_ids)

    def check_throttle(self):
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        command = "{}/bin/kafka-configs.sh --bootstrap-server {} --describe --entity-type brokers".format(
            self.kafka_path, self.bootstrap_servers
        )
        result = self._execute_subprocess(command, env_vars)
        self._parse_describe_brokers_throttle(result.stdout.strip())

    def _parse_describe_brokers_throttle(self, raw_describe_brokers_throttle):
        broker_configs = {}

        # Define regular expressions to extract broker ID and throttled rate
        broker_id_pattern = re.compile(r"broker (\d+)")
        throttled_rate_pattern = re.compile(
            r"leader\.replication\.throttled\.rate=(\d+)"
        )

        # Split stdout into separate blocks for each broker
        broker_blocks = raw_describe_brokers_throttle.split("Dynamic configs for ")
        for block in broker_blocks[1:]:
            broker_id_match = broker_id_pattern.search(block)
            throttled_rate_match = throttled_rate_pattern.search(block)

            # Check if matches were found
            if broker_id_match and throttled_rate_match:
                broker_id = int(broker_id_match.group(1))
                throttled_rate = int(throttled_rate_match.group(1))
                broker_configs[broker_id] = throttled_rate

        print(json.dumps(broker_configs))

    def demote_rollback(self, broker_id, remove_throttle, num_concurrent_leader_movements):
        """
        Perform a rollback for the previous demote operation on a specific broker.

        Args:
            broker_id: The ID of the broker to rollback the demotion for.

        Raises:
            BrokerStatusError: If the previous demote operation on the broker was not found.

        Returns:
            None
        """
        previous_partitions_state = self._remove_non_existent_topics(broker_id)
        if previous_partitions_state is None:
            raise BrokerStatusError(
                "Previous demote operation on broker {} was not found, there is nothing to rollback".format(
                    broker_id
                )
            )
        logger.info(
            "Executing demote rollback operation for broker: {}, it may take a while...".format(
                broker_id
            )
        )
        self._change_replica_assignment(previous_partitions_state)
        self._trigger_leader_election(previous_partitions_state, num_concurrent_leader_movements)
        self._produce_record(broker_id, None)
        if remove_throttle:
            self._unset_throttles(broker_id)
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
            - The generated filepath will have a random filename consisting of lowercase letters and digits.
            - The generated filepath will have the '.json' extension.

        """
        filename = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=10)
        )
        partitions_temp_filepath = tempfile.mktemp(
            suffix=".json", prefix=filename
        )
        self.partitions_temp_filepath = partitions_temp_filepath
        with open(partitions_temp_filepath, "w") as temp_file:
            json.dump(data, temp_file)
            return partitions_temp_filepath

    def _change_replica_assignment(self, demoting_plan):
        """
        Change the replica assignment of partitions based on the provided demoting plan.

        Args:
            demoting_plan (dict): A dictionary containing the demoting plan.

        Raises:
            ChangeReplicaAssignmentError: If an error occurs during the replica assignment change.

        Returns:
            None

        """
        demoting_plan_filepath = self._generate_tempfile_with_json_content(
            demoting_plan
        )
        command = "{}/bin/kafka-reassign-partitions.sh --bootstrap-server {} --reassignment-json-file {} --execute --timeout 60".format(
            self.kafka_path, self.bootstrap_servers, demoting_plan_filepath
        )
        env_vars = os.environ.copy()
        env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
        self._execute_subprocess(command, env_vars)

    def _generate_tmpfile_with_admin_configs(self):
        """
        Generate a temporary file containing the admin configurations.
        The admin configurations are written to the file, and the file
        is closed before returning its name.

        Returns:
            str: The path/name of the temporary file.

        """
        admin_config_tmp_file = tempfile.NamedTemporaryFile(delete=False)

        admin_config_tmp_file.write(self.admin_config_content.encode())
        admin_config_tmp_file.close()
        return admin_config_tmp_file.name

    def _trigger_leader_election(self, demoting_plan, concurrent_leader_movements):
        """
        Trigger a leader election using the demoting plan.

        This method runs the `kafka-leader-election.sh` script with the provided demoting plan.
        The command is executed using the provided Kafka installation's path, the generated admin
        configurations, the bootstrap servers, and the path to the temporary file containing
        the demoting plan in JSON format. The number of leadership movements is limited to concurrent_leader_movements
        to avoid stressing the brokers.

        Args:
            demoting_plan (dict or list): A dictionary or list representation of the demoting plan.
            concurrent_leader_movements (int): Nº of concurrent leader movements between brokers.
        Raises:
            TriggerLeaderElectionError: If the leader election fails.

        """
        # Split entry items into groups of N=concurrent_leader_movements
        grouped_entries = [demoting_plan["partitions"][i:i + concurrent_leader_movements] for i in range(0, len(demoting_plan["partitions"]), concurrent_leader_movements)]

        # Iterate over each group
        for group in grouped_entries:
            logger.info("Running kafka-leader-election on partitions: {}".format(group))
            demoting_plan_filepath = self._generate_tempfile_with_json_content(
                {"partitions": group}
            )

            command = "{}/bin/kafka-leader-election.sh --admin.config {} --bootstrap-server {} --election-type PREFERRED --path-to-json-file {}".format(
                self.kafka_path,
                self.admin_config_tmp_file,
                self.bootstrap_servers,
                demoting_plan_filepath,
            )
            env_vars = os.environ.copy()
            env_vars["KAFKA_HEAP_OPTS"] = self.kafka_heap_opts
            self._execute_subprocess(command, env_vars)
            time.sleep(1)

    def _save_rollback_plan(self, broker_id, current_partitions_state):
        logger.info("Saving rollback plan for broker {}".format(broker_id))
        self._produce_record(broker_id, current_partitions_state)

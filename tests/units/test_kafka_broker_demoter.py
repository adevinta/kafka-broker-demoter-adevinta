import json
import os
import unittest
from unittest.mock import MagicMock, Mock, call, patch

from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from tenacity import stop_after_attempt

# Import the class you want to test (assuming it's in a separate module)
from kafka_broker_demoter.demoter import BrokerStatusError, Demoter
from kafka_broker_demoter.exceptions import ProduceRecordError


class TestDemoter(unittest.TestCase):
    def test_demote_successful_demote_operation(self):
        # Create an instance of Demoter
        broker_id = 45
        demoter = Demoter()
        get_partition_leaders_by_broker_id_result = {"partitions": [1, 2, 3]}
        get_demoting_proposal_result = {"a": 1}

        # Patch the necessary methods to simulate the scenario
        with patch.object(
            demoter, "_create_topic", return_value=None
        ) as mock_create_topic, patch.object(
            demoter, "_consume_latest_record_per_key", return_value=None
        ) as mock_consume_latest_record_per_key, patch.object(
            demoter,
            "_get_partition_leaders_by_broker_id",
            return_value=get_partition_leaders_by_broker_id_result,
        ) as mock_get_partition_leaders_by_broker_id, patch.object(
            demoter, "_get_demoting_proposal", return_value=get_demoting_proposal_result
        ) as mock_get_demoting_proposal, patch.object(
            demoter, "_change_replica_assignment"
        ) as mock_change_replica_assignment, patch.object(
            demoter, "_trigger_leader_election"
        ) as mock_trigger_leader_election, patch.object(
            demoter, "_save_rollback_plan"
        ) as mock_save_rollback_plan:
            # Check if the methods were called correctly in the expected orderi
            mock = MagicMock()
            mock.attach_mock(mock_create_topic, "_create_topic")
            mock.attach_mock(
                mock_consume_latest_record_per_key, "_consume_latest_record_per_key"
            )
            mock.attach_mock(
                mock_get_partition_leaders_by_broker_id,
                "_get_partition_leaders_by_broker_id",
            )
            mock.attach_mock(mock_get_demoting_proposal, "_get_demoting_proposal")
            mock.attach_mock(
                mock_change_replica_assignment, "_change_replica_assignment"
            )
            mock.attach_mock(mock_trigger_leader_election, "_trigger_leader_election")
            mock.attach_mock(mock_save_rollback_plan, "_save_rollback_plan")

            # Call the demote() method with a successful demote operation
            demoter.demote(broker_id=broker_id)

            mock.assert_has_calls(
                [
                    call._create_topic(),
                    call._consume_latest_record_per_key(broker_id),
                    call._get_partition_leaders_by_broker_id(broker_id),
                    call._get_demoting_proposal(
                        broker_id, get_partition_leaders_by_broker_id_result
                    ),
                    call._change_replica_assignment(get_demoting_proposal_result),
                    call._trigger_leader_election(get_demoting_proposal_result),
                    call._save_rollback_plan(
                        broker_id, get_partition_leaders_by_broker_id_result
                    ),
                ],
                any_order=False,
            )

    def test_demote_rollback_success(self):
        # Dummy data
        broker_id = 123
        previous_partitions_state = "previous_state"

        with patch.object(
            Demoter,
            "_remove_non_existent_topics",
            return_value=previous_partitions_state,
        ):
            with patch.object(
                Demoter, "_change_replica_assignment"
            ) as mock_change_replica_assignment, patch.object(
                Demoter, "_trigger_leader_election"
            ) as mock_trigger_leader_election, patch.object(
                Demoter, "_produce_record"
            ) as mock_produce_record:
                # Create an instance of Demoter
                demoter = Demoter()

                # Call the demote_rollback() method
                demoter.demote_rollback(broker_id)

                # Verify the method calls and assertions
                mock_change_replica_assignment.assert_called_once_with(
                    previous_partitions_state
                )
                mock_trigger_leader_election.assert_called_once_with(
                    previous_partitions_state
                )
                mock_produce_record.assert_called_once_with(broker_id, None)

    def test_demote_rollback_failure(self):
        # Dummy data
        broker_id = 123
        previous_partitions_state = None

        with patch.object(
            Demoter,
            "_remove_non_existent_topics",
            return_value=previous_partitions_state,
        ):
            with self.assertRaises(BrokerStatusError):
                # Create an instance of Demoter
                demoter = Demoter()

                # Call the demote_rollback() method
                demoter.demote_rollback(broker_id)

    def test_generate_tempfile_with_json_content(self):
        demoter = Demoter()
        data = {"key1": "value1", "key2": "value2", "key3": "value3"}

        expected_contents = json.dumps(data)

        actual_result = demoter._generate_tempfile_with_json_content(data)

        with open(actual_result, "r") as actual_file:
            # Assert that the contents of the file match the expected contents
            self.assertEqual(actual_file.read(), expected_contents)

    def test_generate_tmpfile_with_admin_configs(self):
        demoter = Demoter()
        expected_content = demoter.admin_config_content

        admin_config_path = demoter._generate_tmpfile_with_admin_configs()

        with open(admin_config_path, "r") as actual_file:
            # Assert that the contents of the file match the expected contents
            self.assertEqual(actual_file.read(), expected_content)

    @patch("subprocess.run")
    @patch("os.environ.copy")
    def test_change_replica_assignment(self, mock_environ_copy, mock_subprocess_run):
        kafka_path = "/path/to/kafka"
        bootstrap_servers = "localhost:9092"
        kafka_heap_opts = "-Xmx1G"
        demoting_plan = {"broker_id": 1}

        # Mock subprocess.run() method behavior
        mock_subprocess_run.return_value = MagicMock(
            returncode=0,
            stdout="Leader reassignment successful",
            stderr="",
        )

        demoter = Demoter(
            kafka_path=kafka_path,
            bootstrap_servers=bootstrap_servers,
            kafka_heap_opts=kafka_heap_opts,
        )
        demoter._change_replica_assignment(demoting_plan)

        # Generate the command and expected environment variables
        expected_command = "{}/bin/kafka-reassign-partitions.sh --bootstrap-server {} --reassignment-json-file {} --execute --timeout 60".format(
            kafka_path,
            bootstrap_servers,
            demoter.partitions_temp_filepath,
        )

        expected_env_vars = os.environ.copy()
        expected_env_vars["KAFKA_HEAP_OPTS"] = kafka_heap_opts

        # Assert that subprocess.run() is called with correct parameters
        mock_subprocess_run.assert_called_with(
            expected_command,
            shell=True,
            capture_output=True,
            text=True,
            env=expected_env_vars,
        )

    @patch("subprocess.run")
    @patch("os.environ.copy")
    def test_trigger_leader_election(self, mock_environ_copy, mock_subprocess_run):
        kafka_path = "/path/to/kafka"
        bootstrap_servers = "localhost:9092"
        kafka_heap_opts = "-Xmx1G"
        demoting_plan = {"broker_id": 1}

        # Mock subprocess.run() method behavior
        mock_subprocess_run.return_value = MagicMock(
            returncode=0,
            stdout="Leader election successful",
            stderr="",
        )

        demoter = Demoter(
            kafka_path=kafka_path,
            bootstrap_servers=bootstrap_servers,
            kafka_heap_opts=kafka_heap_opts,
        )
        demoter._trigger_leader_election(demoting_plan)

        # Generate the command and expected environment variables
        expected_command = "{}/bin/kafka-leader-election.sh --admin.config {} --bootstrap-server {} --election-type PREFERRED --path-to-json-file {}".format(
            kafka_path,
            demoter.admin_config_tmp_file.name,
            bootstrap_servers,
            demoter.partitions_temp_filepath,
        )

        expected_env_vars = os.environ.copy()
        expected_env_vars["KAFKA_HEAP_OPTS"] = kafka_heap_opts

        # Assert that subprocess.run() is called with correct parameters
        mock_subprocess_run.assert_called_with(
            expected_command,
            shell=True,
            capture_output=True,
            text=True,
            env=expected_env_vars,
        )

    def test_get_partition_leaders_by_broker_id(self):
        demoter = Demoter()
        existing_topics = [
            {
                "topic": "topic1",
                "partitions": [
                    {"partition": 0, "leader": 1, "replicas": [1, 2, 3]},
                    {"partition": 1, "leader": 2, "replicas": [2, 3, 4]},
                ],
            },
            {
                "topic": "topic2",
                "partitions": [
                    {"partition": 0, "leader": 2, "replicas": [2, 1, 3]},
                    {"partition": 1, "leader": 1, "replicas": [1, 3, 4]},
                ],
            },
        ]

        with patch.object(
            Demoter, "_get_topics_metadata", return_value=existing_topics
        ):
            # No leaders for broker 3
            partition_leaders = demoter._get_partition_leaders_by_broker_id(3)
            self.assertEqual(partition_leaders, {"partitions": []})
            # Partition leaders for broker 1
            partition_leaders = demoter._get_partition_leaders_by_broker_id(1)
            self.assertEqual(
                partition_leaders,
                {
                    "partitions": [
                        {"partition": 0, "topic": "topic1", "replicas": [1, 2, 3]},
                        {"partition": 1, "topic": "topic2", "replicas": [1, 3, 4]},
                    ]
                },
            )

    def test_demoting_proposal(self):
        demoter = Demoter()
        broker_id = 2
        current_partitions_state = {
            "topic": "topic1",
            "partitions": [
                {"partition": 0, "leader": 2, "replicas": [2, 1, 3]},
                {"partition": 1, "leader": 2, "replicas": [1, 2, 3]},
            ],
        }
        expected_proposal = {
            "topic": "topic1",
            "partitions": [
                {"partition": 0, "leader": 2, "replicas": [3, 2, 1]},
                {"partition": 1, "leader": 2, "replicas": [3, 1, 2]},
            ],
        }
        proposal = demoter._get_demoting_proposal(broker_id, current_partitions_state)
        self.assertEqual(proposal, expected_proposal)

    def _create_topic(self):
        topics = self._get_admin_client.list_topics()
        if self.topic_tracker not in topics:
            print(
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

    def test_produce_record(self):
        demoter = Demoter()

        # Creating a mock producer and future
        mock_future = Mock()
        mock_future.get.return_value = Mock()  # Mocking the returned record metadata

        with patch.object(Demoter, "_get_producer") as mock_producer:
            mock_producer.return_value.send.return_value = mock_future
            demoter._produce_record("key", "value")

        # Asserting that the producer's send method is called with the expected parameters
        mock_producer.return_value.send.assert_called_once_with(
            demoter.topic_tracker,
            key=b"key",
            value=b'"value"',
        )

    def test_produce_record_exception(self):
        demoter = Demoter()

        demoter._produce_record.retry.stop = stop_after_attempt(1)
        mock_future = Mock()
        mock_future.get.side_effect = ProduceRecordError

        with patch.object(Demoter, "_get_producer") as mock_producer:
            mock_producer.return_value.send.return_value = mock_future
            with self.assertRaises(ProduceRecordError):
                demoter._produce_record("key", "value")

    def test_remove_non_existent_topics(self):
        demoter = Demoter()
        broker_id = 1
        latest_record_per_key = {
            "partitions": [
                {"topic": "topic1"},
                {"topic": "topic2"},
                {"topic": "topic3"},
                {"topic": "topic4"},
            ]
        }
        existing_topics = [
            {"topic": "topic2"},
            {"topic": "topic3"},
        ]

        with patch.object(
            Demoter,
            "_consume_latest_record_per_key",
            return_value=latest_record_per_key,
        ), patch.object(Demoter, "_get_topics_metadata", return_value=existing_topics):
            result = demoter._remove_non_existent_topics(broker_id)

        expected_result = {"partitions": [{"topic": "topic2"}, {"topic": "topic3"}]}

        # Assert the result matches the expected result
        self.assertEqual(result, expected_result)

    def test_consume_latest_record_per_key(self):
        sample_records = {
            "topic_name": [
                ConsumerRecord(
                    topic="topic_name",
                    partition=0,
                    offset=0,
                    timestamp=1602632031000,
                    timestamp_type=0,
                    key=b"10",
                    value=b'{"data": "value_1"}',
                    headers=[],
                    checksum=0,
                    serialized_key_size=0,
                    serialized_value_size=0,
                    serialized_header_size=0,
                ),
                ConsumerRecord(
                    topic="topic_name",
                    partition=0,
                    offset=1,
                    timestamp=1602632032000,
                    timestamp_type=0,
                    key=b"11",
                    value=b'{"data": "value_2"}',
                    headers=[],
                    checksum=0,
                    serialized_key_size=0,
                    serialized_value_size=0,
                    serialized_header_size=0,
                ),
                ConsumerRecord(
                    topic="topic_name",
                    partition=0,
                    offset=2,
                    timestamp=1602632033000,
                    timestamp_type=0,
                    key=b"10",
                    value=b'{"data": "value_3"}',
                    headers=[],
                    checksum=0,
                    serialized_key_size=0,
                    serialized_value_size=0,
                    serialized_header_size=0,
                ),
            ]
        }

        with patch.object(Demoter, "_get_consumer") as mock_get_consumer:
            mock_get_consumer.return_value.poll.return_value = sample_records

            # Set the desired return value for consumer.poll()
            # desired_records = {"record1", "record2"}
            # mock_poll.return_value = sample_records

            # Create an instance of Demoter
            demoter = Demoter()

            # Call the _consume_latest_record_per_key method with various keys
            # Test with a key that exists in the received records
            result = demoter._consume_latest_record_per_key(10)
            expected_result = {"data": "value_3"}
            self.assertEqual(result, expected_result)

            # Test with a key that is not present in the received records
            result = demoter._consume_latest_record_per_key(4)
            expected_result = None
            self.assertEqual(result, expected_result)

            # Test with an empty record
            demoter._get_consumer.poll.return_value = {}
            result = demoter._consume_latest_record_per_key(2)
            expected_result = None
            self.assertEqual(result, expected_result)

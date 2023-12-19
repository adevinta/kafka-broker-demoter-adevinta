import json
import unittest
from unittest.mock import MagicMock, patch

from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord

# Import the class you want to test (assuming it's in a separate module)
from kafka_broker_demoter.demoter import Demoter
from kafka_broker_demoter.exceptions import PreferredLeaderMismatchCurrentLeader

# from kafka import KafkaConsumer


class TestDemoter(unittest.TestCase):
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
        # Mismatch current leader and preferred leader
        existing_topics = [
            {
                "topic": "topic1",
                "partitions": [{"partition": 0, "leader": 1, "replicas": [2, 2, 3]}],
            }
        ]
        with patch.object(
            Demoter, "_get_topics_metadata", return_value=existing_topics
        ):
            with self.assertRaises(PreferredLeaderMismatchCurrentLeader):
                demoter._get_partition_leaders_by_broker_id(1)

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
        demoter._get_producer = MagicMock()

        key = "test-key"
        value = {"field1": "value1", "field2": "value2"}

        demoter._get_producer.return_value.send = MagicMock()
        demoter._get_producer.return_value.flush = MagicMock()
        demoter._get_producer.return_value.close = MagicMock()

        demoter._produce_record(key, value)

        demoter._get_producer.assert_called_once()
        demoter._get_producer.return_value.send.assert_called_once_with(
            demoter.topic_tracker,
            key="test-key".encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
        )
        demoter._get_producer.return_value.flush.assert_called_once()
        demoter._get_producer.return_value.close.assert_called_once()

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

            # Create an instance of YourClass
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

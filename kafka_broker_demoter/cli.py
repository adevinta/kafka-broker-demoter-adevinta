import argparse
import logging

from kafka_broker_demoter.configure_logging import configure_logging
from kafka_broker_demoter.demoter import Demoter

logger = logging.getLogger(__name__)


def parseargs():
    parser = argparse.ArgumentParser(description="Kafka Broker Demoter")
    parser.add_argument(
        "demotion_action",
        choices=["demote", "demote_rollback"],
        help="The action to perform: demote or demote_rollback",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Sets the Kafka bootstrap servers",
        required=False,
    )
    parser.add_argument(
        "--broker-id",
        type=int,
        help="The ID of the broker to be demoted or rollback demote",
        required=True,
    )
    parser.add_argument(
        "--kafka-path",
        type=str,
        default="/usr/local/kafka",
        help="Sets the Kafka installation path",
        required=False,
    )
    parser.add_argument(
        "--kafka-heap-opts",
        type=str,
        default="-Xmx512M",
        help="Sets the Kafka heap options",
        required=False,
    )
    parser.add_argument(
        "--topic-tracker",
        type=str,
        default="__demote_tracker",
        help="Sets the topic used to track broker demotion actions",
        required=False,
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="/var/log/kafka_broker_demoter.log",
        help="Sets the log file.",
        required=False,
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["INFO", "DEBUG"],
        default="INFO",
        help="Sets the log level",
        required=False,
    )

    return parser.parse_args()


def main():
    args = parseargs()
    configure_logging(args.log_level, args.log_file)

    demoter = Demoter(
        args.bootstrap_servers,
        args.kafka_path,
        args.kafka_heap_opts,
        args.topic_tracker,
    )

    if args.demotion_action == "demote":
        demoter.demote(args.broker_id)
    if args.demotion_action == "demote_rollback":
        demoter.demote_rollback(args.broker_id)

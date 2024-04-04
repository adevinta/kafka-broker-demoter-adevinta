import argparse
import logging
import sys

from kafka_broker_demoter.configure_logging import configure_logging
from kafka_broker_demoter.demoter import Demoter

logger = logging.getLogger(__name__)


def parseargs():
    parser = argparse.ArgumentParser(description="Kafka Broker Demoter")
    parser.add_argument(
        "demotion_action",
        choices=["demote", "demote_rollback", "update_throttle", "check_throttle"],
        help="The action to perform: demote, demote_rollback, update_throttle or check_throttle",
    )
    # Create an argument group for demote-related arguments
    demote_group = parser.add_argument_group("demote")
    demote_group.add_argument(
        "--throttle-bytes",
        type=int,
        help="The max bytes per second each broker will either send or recieve for data replication (it does not affect client producer or consumers)",
        default=-1,
    )
    # Create an argument group for demote_rollback-related arguments
    demote_rollback_group = parser.add_argument_group("demote_rollback")
    demote_rollback_group.add_argument(
        "--remove-throttle",
        action="store_true",
        default=False,
        required=False,
        help="Indicates whether to remove the throttle for a previous demote operation",
    )
    # Create an argument group for update_throttle-related arguments
    update_throttle_group = parser.add_argument_group("update_throttle")
    update_throttle_group.add_argument(
        "--update-throttle-bytes",
        type=int,
        default=False,
        required=("update_throttle" in sys.argv),
        help="Updates the max bytes per second each broker will either send or recieve for data replication (it does not affect client producer or consumers)",
    )
    update_throttle_group.add_argument(
        "--broker_ids",
        nargs="+",
        default=[],
        required=False,
        help="The specific broker ids (space separated) to apply the throttle, by default it's apply in all of them",
    )

    # Global arguments
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
        help="The ID of the broker to be demoted, rollback or update a throttle.",
        required=(
            "demote" in sys.argv
            or "demote_rollback" in sys.argv
            or "update_throttle" in sys.argv
        ),
    )
    parser.add_argument(
        "--concurrent-leader-movements",
        type=int,
        help="The max nº of leader movements that will be carried out at the same time.",
        default=1,
        required=False,
    )
    parser.add_argument(
        "--kafka-path",
        type=str,
        default="/opt/kafka",
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
        demoter.demote(args.broker_id, args.throttle_bytes, args.concurrent_leader_movements)
    elif args.demotion_action == "demote_rollback":
        demoter.demote_rollback(args.broker_id, args.remove_throttle, args.concurrent_leader_movements)
    elif args.demotion_action == "update_throttle":
        # Convert broker_ids to a list of integers
        if args.broker_ids:
            args.broker_ids = [int(broker_id) for broker_id in args.broker_ids]
        demoter.update_throttle(
            args.broker_id, args.update_throttle_bytes, args.broker_ids
        )
    elif args.demotion_action == "check_throttle":
        demoter.check_throttle()

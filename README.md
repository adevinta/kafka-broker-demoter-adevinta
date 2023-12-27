# Kafka Broker Demoter

Kafka Broker Demoter is a script that allows you to demote Kafka brokers and roll back the changes as needed. This script can be installed via `pip` by running the following command:

```
pip install kafka-broker-demoter
```

After installation, you will have access to the `kafka_broker_demoter` script, located at `/usr/local/bin/kafka_broker_demoter`. This tool provides the following options:

```
usage: kafka_broker_demoter [-h] [--bootstrap-servers BOOTSTRAP_SERVERS]
                            --broker-id BROKER_ID [--kafka-path KAFKA_PATH]
                            [--kafka-heap-opts KAFKA_HEAP_OPTS]
                            [--topic-tracker TOPIC_TRACKER]
                            [--log-file LOG_FILE] [--log-level {INFO,DEBUG}]
                            {demote,demote_rollback}
```

## Installation

To install Kafka Broker Demoter, simply run the following command:

```
pip install kafka-broker-demoter
```

Make sure you have `pip` installed on your system before running this command.

## Usage

The Kafka Broker Demoter tool provides two actions: `demote` and `demote_rollback`. To use any of the actions, you need to specify the `--broker-id` parameter. Here are the available options:

### Demote Action

```
kafka_broker_demoter demote --broker-id <BROKER_ID>
```

The demote action will lower the priority of the specified broker, allowing other brokers to take on its partitions leaderships. This is achieved by moving all partition leaders to the replicas with higher priority, and adjusting the preferred leader to avoid sudden rollbacks triggered by the controller.

This action is useful in scenarios such as copying a large volume of data, where the partitions on the demoted broker may be in sync and competing for resources with under-replicated partitions. By demoting the broker, you can distribute the load more evenly and leave the demoter broker leaderless until the rollback is executed.

### Demote Rollback Action

```
kafka_broker_demoter demote_rollback --broker-id <BROKER_ID>
```

The demote rollback action will revert the changes made by the demote action, restoring the original state of the broker.

## Configuration Options

- `--bootstrap-servers`: The list of Kafka brokers to connect to. Required for both actions.
- `--kafka-path`: The path to the Kafka installation directory. Default: /usr/local/kafka.
- `--kafka-heap-opts`: The heap options to use when starting Kafka. Default: -Xmx512M.
- `--topic-tracker`: The topic tracker to use. Default: `default`.
- `--log-file`: The path to the log file. Default: kafka_broker_demoter.log.
- `--log-level`: The log level. Available options: INFO, DEBUG. Default: INFO.

## License

This script is released under the [Apache License 2.0](LICENSE.txt).

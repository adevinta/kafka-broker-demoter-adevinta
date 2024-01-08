class SetBrokerThrottleError(Exception):
    pass


class SetTopicThrottleError(Exception):
    pass


class RecordNotFoundError(Exception):
    pass


class BrokerStatusError(Exception):
    pass


class TriggerLeaderElectionError(Exception):
    pass


class ProduceRecordError(Exception):
    pass


class ChangeReplicaAssignmentError(Exception):
    pass

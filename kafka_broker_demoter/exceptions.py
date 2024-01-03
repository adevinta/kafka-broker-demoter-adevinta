class BrokerStatusError(Exception):
    pass


class TriggerLeaderElectionError(Exception):
    pass


class ProduceRecordError(Exception):
    pass


class ChangeReplicaAssignmentError(Exception):
    pass

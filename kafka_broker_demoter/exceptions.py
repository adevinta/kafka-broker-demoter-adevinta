class PreferredLeaderMismatchCurrentLeader(Exception):
    pass


class BrokerStatusError(Exception):
    pass


class TriggerLeaderElectionError(Exception):
    pass


class ChangeReplicaAssignmentError(Exception):
    pass

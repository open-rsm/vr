package vr

import (
	"testing"
	"github.com/open-rsm/spec/proto"
)

// Proof: section 3
// State machine replication requires that replicas start in
// the same initial state, and that operations be deterministic.
func TestSameInitialState(t *testing.T) {

}

// Proof: section 3
// VR uses a primary replica to order client requests; the
// other replicas are backups that simply accept the order
// selected by the primary.
func TestPrimaryDeterminesLogOrder(t *testing.T) {

}

// Proof: section 3
// The backups monitor the primary, and if it appears to be
// faulty, they carry out a view change protocol to select
// a new primary.

// Proof: section 4.1
// Replicas only process normal protocol messages containing
// a view-number that matches the view-number they know.
// If the sender is behind, the receiver routers the message.
func TestSenderBehindDropsMessage(t *testing.T) {
	roles := []role{Replica, Primary, Backup}
	for _, role := range roles {
		testSenderBehindDropsMessage(t, role)
	}
}

// Proof: section 4.1
// If the sender is ahead, the replica performs a state
// transfer: it requests information it is missing from the
// other replicas and uses this information to bring itself
// up to date before processing the message.
func TestSenderAheadPerformsStateTransfer(t *testing.T) {
	roles := []role{Replica, Primary, Backup}
	for _, role := range roles {
		testSenderAheadPerformsStateTransfer(t, role)
	}
}

func testSenderBehindDropsMessage(t *testing.T, s role) {
	r := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1, 2, 3},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             NewStore(),
		AppliedNum:        0,
	})
	switch s {
	case Replica:
		r.becomeReplica()
	case Backup:
		r.becomeBackup(1, 2)
	case Primary:
		r.becomeReplica()
		r.becomePrimary()
	}
	r.Call(proto.Message{Type: proto.Prepare, ViewNum: 2})
	if r.ViewNum != 2 {
		t.Errorf("view-number = %d, expected %d", r.ViewNum, 2)
	}
	if r.role != Backup {
		t.Errorf("s = %v, expected %v", r.role, Backup)
	}
}

func testSenderAheadPerformsStateTransfer(t *testing.T, s role) {

}

// Proof: section 4.1
// The primary advances op-number, adds the request to the end
// of the opLog, Then it sends a <PREPARE v, m, n, k> message to
// the other replicas, where v is the current view-number, m is
// the message it received from the client, n is the op-number
// it assigned to the request, and k is the commitNum-number.
// The primary waits for f PREPARE_OK messages from different
// backups; at this point it considers the operation (and all
// earlier ones) to be committed. Then, after it has executed
// all earlier operations (those assigned smaller op-numbers),
// the primary executes the operation by making an up-call to
// the service code, and increments its commitNum-number.
func TestPrimarySyncPrepareToBackups(t *testing.T) {
	testPrimarySendPrepare()
	testPrimaryWaitsPrepareOk()
}

func testPrimarySendPrepare() {

}

func testPrimaryWaitsPrepareOk() {

}

// Backups process PREPARE messages in order: a backup won’t accept
// a prepare with op-number n until it has entries for all earlier
// requests in its opLog. When a backup i receives a PREPARE message,
// it waits until it has entries in its opLog for all earlier requests
// (doing state transfer if necessary to get the missing information).
// Then it increments its op-number, adds the request to the end of
// its opLog, updates the client’s information in the client-table, and
// sends a <PREPARE_OK v, n, i> message to the primary to indicate
// that this operation and all earlier ones have prepared locally
func TestBackupsProcessPrepareFromPrimary(t *testing.T) {

}

// Normally the primary informs backups about the commitNum when it
// sends the next PREPARE message; this is the purpose of the
// commitNum-number in the PREPARE message. However, if the primary
// does not receive a new client request in a timely way, it instead
// informs the backups of the latest commitNum by sending them a <COMMIT
// v, k> message, where k is commitNum-number (note that in this case
// commitNum-number = op-number).
func TestPrimaryHeartbeatToBackups(t *testing.T) {

}

// When a backup learns of a commitNum, it waits until it has the request
// in its opLog (which may require state transfer) and until it has
// executed all earlier operations. Then it executes the operation by
// performing the up-call to the service code, increments its commitNum-number,
// updates the client’s entry in the client-table, but does not send the
// reply to the client.
func TestBackupCommitLogApplyToStore(t *testing.T) {

}

// Proof: section 4.2
// If a timeout expires without a communication from the primary, the
// replicas carry out a view change to switch to a new primary.
func TestBackupsTriggerTransitionToPrimary(t *testing.T) {
	// phase 1
	// phase 2
	// phase 3
	// phase 4
	// phase 5
}

// Proof: section 4.2
// up-calls occur only for committed operations. This means that the old
// primary must have received at least f PREPARE_OK messages from other
// replicas, and this in turn implies that the operation is recorded in
// the logs of at least f + 1 replicas (the old primary and the f backups
// that sent the PREPARE_OK messages).
func TestLogCommittedByReplicas(t *testing.T) {

}

// Proof: section 4.2
// Suppose an operation is still in the preparing stage and has not been
// written to the majority opLog. At this time, a view change occurs. If
// each replica in the new view has not received the op, the op will be
// lost. There is no correctness problem.
func TestLostPreparingLogInViewChange(t *testing.T) {

}

// Proof: section 4.2
// Is it possible that two different operations have the same op-number?
// Possibly, these two operations must belong to two different views, so they
// have different view numbers. The larger view number is required. The VR
// protocol can ensure that this situation will not occur under the same view
// number.
func TestOpNumberDuplicateProblem(t *testing.T) {

}

// Proof: section 4.3
// When a replica recovers after a crash it cannot participate in request
// processing and view changes until it has a state at least as recent as
// when it failed. If it could participate sooner than this, the system can
// fail.
func TestFailedNodeRejoinsCluster(t *testing.T) {
	// phase 1
	// phase 2
	// phase 3
}

// Proof: section 4.3
// A failed node cannot participate in view change before recovery is complete.
func TestRecoveryReplicaIgnoreViewChange(t *testing.T) {

}

// Proof: section 5.2
// State transfer is used to track data from backward replicas in non-crash
// scenarios. There are two cases.

// In the current view, you are behind. In this case, you only need to fill
// in the opLog after your op-number.
func TestSyncBackwardLogs(t *testing.T) {
	// phase 1
	// phase 2
	// phase 3
}

// A view change has occurred, and you are no longer in the new view. In this
// case, you need to truncate your opLog to commitNum-number (because the following
// op may be rewritten in the new view), and then find other replicas to pull
// the opLog.
func TestTruncateAndSyncLatestLog(t *testing.T) {

}

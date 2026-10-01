package workflowprobe

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeProfileUpdate() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	// Enroll two ready nodes; their names stay unique for the whole scenario.
	target, err := enrollNode(harness, "resolver-move-01", "east")
	if err != nil {
		return err
	}
	other, err := enrollNode(harness, "resolver-keep-01", "west")
	if err != nil {
		return err
	}
	originalID := target.ID

	// First enrollment keeps the original conflict behavior.
	_, err = enrollNode(harness, "resolver-move-01", "east")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeConflict)); err != nil {
		return err
	}

	// A stale version is rejected before any field changes.
	_, err = updateNode(harness, "resolver-move-01", "north", false, target.Version+1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	// Controlled region move keeps the identifier and bumps the version.
	moved, err := updateNode(harness, "resolver-move-01", "north", false, target.Version)
	if err != nil {
		return err
	}
	if moved.ID != originalID {
		return ensure(false, "node identifier changed after profile update")
	}
	if moved.Region != "north" || moved.State != domain.NodeReady {
		return ensure(false, "region move did not apply to the profile")
	}
	if moved.Version != target.Version+1 {
		return ensure(false, "profile update did not increment the version")
	}
	if moved.CreatedAt != target.CreatedAt {
		return ensure(false, "profile update rewrote the original enrollment time")
	}

	// A no-op replay at the new version is idempotent and does not advance.
	replay, err := updateNode(harness, "resolver-move-01", "north", false, moved.Version)
	if err != nil {
		return err
	}
	if replay.Version != moved.Version {
		return ensure(false, "no-op node update advanced the version")
	}

	// Looking up an unknown name is a clear not-found result.
	_, err = updateNode(harness, "resolver-missing-01", "north", false, 1)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	// Bad input (short region / missing version) is a validation failure.
	err = harness.call(http.MethodPatch, "/v1/nodes", map[string]any{
		"node_name":        "resolver-move-01",
		"region":           "n",
		"expected_version": moved.Version,
	}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// Leasing the moved node locks its profile for the duration of the lease.
	plan, _, err := createActivePlan(harness, "nodemove.example", 3001, 1, 1, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "node-move-lease-001", "north", 10)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 1 {
		return ensure(false, "new region must select exactly the moved node")
	}

	// Leasing bumps the node version; read the current leased profile.
	var leasedPage domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=leased&region=north&limit=10", nil, &leasedPage); err != nil {
		return err
	}
	if len(leasedPage.Items) != 1 || leasedPage.Items[0].ID != originalID {
		return ensure(false, "leased node listing did not return the moved identity")
	}
	leasedVersion := leasedPage.Items[0].Version

	// The leased node cannot be corrected while its lease is active.
	_, err = updateNode(harness, "resolver-move-01", "south", false, leasedVersion)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	_, err = updateNode(harness, "resolver-move-01", "south", false, moved.Version)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	// Releasing the lease makes the node updatable again, and allocation now
	// follows the corrected region rather than the enrollment region.
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.NodeID != originalID {
		return ensure(false, "historical lease no longer references the same node identity")
	}
	_, err = allocate(harness, plan.ID, "node-move-lease-002", "north", 10)
	if err != nil {
		return err
	}
	_, err = allocate(harness, plan.ID, "node-move-lease-003", "east", 10)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}

	// A different ready node can be escalated to quarantine.
	quarantined, err := updateNode(harness, "resolver-keep-01", "west", true, other.Version)
	if err != nil {
		return err
	}
	if quarantined.State != domain.NodeQuarantined || quarantined.Version != other.Version+1 {
		return ensure(false, "quarantine escalation did not take effect")
	}
	if quarantined.ID != other.ID {
		return ensure(false, "quarantine escalation changed the node identifier")
	}

	// A quarantined profile is locked: neither correction nor de-escalation.
	_, err = updateNode(harness, "resolver-keep-01", "central", false, quarantined.Version)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	_, err = updateNode(harness, "resolver-keep-01", "west", false, quarantined.Version)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Quarantined nodes stay excluded from allocation.
	_, err = allocate(harness, plan.ID, "node-move-lease-004", "west", 10)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	return nil
}

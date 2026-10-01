package workflowprobe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeProfileUpdate() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	node, err := enrollNode(harness, "resolver-update-01", "east")
	if err != nil {
		return err
	}

	// 重复登记保持原有冲突结果。
	var duplicate domain.ResolverNode
	err = harness.call(http.MethodPut, "/v1/nodes", domain.NodeCreateInput{
		NodeName: "resolver-update-01",
		Region:   "east",
	}, &duplicate)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeConflict)); err != nil {
		return err
	}

	// 版本不匹配与未知名称都有明确结果。
	err = harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"region":           "west",
		"expected_version": node.Version + 1,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/nodes/resolver-missing-01", map[string]any{
		"region":           "west",
		"expected_version": 1,
	}, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"expected_version": node.Version,
	}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// 区域受控更新：标识不变，版本递增。
	var moved domain.ResolverNode
	if err := harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"region":           "west",
		"expected_version": node.Version,
	}, &moved); err != nil {
		return err
	}
	if moved.ID != node.ID || moved.NodeName != node.NodeName || !moved.CreatedAt.Equal(node.CreatedAt) {
		return ensure(false, "node identity changed during profile update")
	}
	if moved.Region != "west" || moved.Version != node.Version+1 {
		return ensure(false, "region update did not apply with a version increment")
	}

	// 后续分配使用新区域。
	plan, _, err := createActivePlan(harness, "update.example", 7001, 1, 1, 100)
	if err != nil {
		return err
	}
	_, err = allocate(harness, plan.ID, "update-lease-0001", "east", 1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "update-lease-0002", "west", 1)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 1 {
		return ensure(false, "allocation in the new region did not lease the node")
	}

	// leased 节点拒绝更新。
	current, err := findNode(harness, node.ID)
	if err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"region":           "east",
		"expected_version": current.Version,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// 释放租约后可以隔离升级，隔离节点不参与分配。
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	current, err = findNode(harness, node.ID)
	if err != nil {
		return err
	}
	var quarantined domain.ResolverNode
	if err := harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"quarantine":       true,
		"expected_version": current.Version,
	}, &quarantined); err != nil {
		return err
	}
	if quarantined.State != domain.NodeQuarantined {
		return ensure(false, "quarantine escalation did not update node state")
	}
	_, err = allocate(harness, plan.ID, "update-lease-0003", "west", 1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	var restored domain.ResolverNode
	if err := harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"quarantine":       false,
		"expected_version": quarantined.Version,
	}, &restored); err != nil {
		return err
	}
	if restored.State != domain.NodeReady {
		return ensure(false, "quarantine lift did not restore ready state")
	}

	// 已到期租约不阻塞更新：更新事务内先行过期清理。
	var shortGroup domain.LeaseGroup
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/leases", domain.LeaseInput{
		RequestKey:   "update-lease-0004",
		Region:       "west",
		LeaseSeconds: 30,
		MaxNodes:     1,
	}, &shortGroup); err != nil {
		return err
	}
	harness.clock.Advance(31 * time.Second)
	current, err = findNode(harness, node.ID)
	if err != nil {
		return err
	}
	var relocated domain.ResolverNode
	if err := harness.call(http.MethodPatch, "/v1/nodes/resolver-update-01", map[string]any{
		"region":           "central",
		"expected_version": current.Version,
	}, &relocated); err != nil {
		return err
	}
	if relocated.Region != "central" || relocated.State != domain.NodeReady {
		return ensure(false, "update after lease expiry did not apply")
	}
	if _, err := allocate(harness, plan.ID, "update-lease-0005", "central", 1); err != nil {
		return err
	}

	// 快照中的审计事件记录字段前后值，历史租约仍引用当时节点资料。
	return verifyNodeUpdateSnapshot(harness, node.ID, group.LeaseIDs[0], shortGroup.LeaseIDs[0])
}

func findNode(harness *harness, nodeID string) (domain.ResolverNode, error) {
	var page domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?limit=100", nil, &page); err != nil {
		return domain.ResolverNode{}, err
	}
	for _, item := range page.Items {
		if item.ID == nodeID {
			return item, nil
		}
	}
	return domain.ResolverNode{}, fmt.Errorf("node %s missing from list", nodeID)
}

func verifyNodeUpdateSnapshot(harness *harness, nodeID string, releasedLeaseID string, expiredLeaseID string) error {
	payload, err := os.ReadFile(harness.tempDir + "/state.json")
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	var snapshot domain.State
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	updates := make([]domain.AuditEvent, 0)
	for _, event := range snapshot.AuditEvents {
		if event.EntityType == "node" && event.EntityID == nodeID && event.Action == "updated" {
			updates = append(updates, event)
		}
	}
	if len(updates) != 4 {
		return ensure(false, "expected four node update audit events")
	}
	if err := expectChange(updates[0], "region", "east", "west"); err != nil {
		return err
	}
	if err := expectChange(updates[1], "state", "ready", "quarantined"); err != nil {
		return err
	}
	if err := expectChange(updates[2], "state", "quarantined", "ready"); err != nil {
		return err
	}
	if err := expectChange(updates[3], "region", "west", "central"); err != nil {
		return err
	}
	released, ok := snapshot.Leases[releasedLeaseID]
	if !ok || released.NodeID != nodeID || released.State != domain.LeaseReleased {
		return ensure(false, "historical released lease no longer references the node")
	}
	expired, ok := snapshot.Leases[expiredLeaseID]
	if !ok || expired.NodeID != nodeID || expired.State != domain.LeaseExpired {
		return ensure(false, "expired lease no longer references the node")
	}
	return nil
}

func expectChange(event domain.AuditEvent, field string, before string, after string) error {
	changes, ok := event.Attributes["changes"].(map[string]any)
	if !ok {
		return fmt.Errorf("audit event %s has no recorded changes", event.ID)
	}
	entry, ok := changes[field].(map[string]any)
	if !ok {
		return fmt.Errorf("audit event %s has no %s change", event.ID, field)
	}
	if entry["before"] != before || entry["after"] != after {
		return fmt.Errorf(
			"audit event %s recorded %s %v -> %v, expected %s -> %s",
			event.ID,
			field,
			entry["before"],
			entry["after"],
			before,
			after,
		)
	}
	return nil
}

package fleet

import (
	"context"
	"sort"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Enroll(ctx context.Context, input domain.NodeCreateInput) (domain.ResolverNode, error) {
	if err := domain.ValidateNodeInput(input, s.clock.Now()); err != nil {
		return domain.ResolverNode{}, err
	}
	input = domain.NormalizeNodeInput(input)
	now := s.clock.Now().UTC()
	state := domain.NodeReady
	if input.Quarantine {
		state = domain.NodeQuarantined
	}
	node := domain.ResolverNode{
		ID:        s.ids.New("nod"),
		NodeName:  input.NodeName,
		Region:    input.Region,
		State:     state,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	err := s.store.Write(ctx, func(state *domain.State) error {
		for _, existing := range state.Nodes {
			if existing.NodeName == node.NodeName {
				return domain.NewError(domain.CodeConflict, "node name is already enrolled", map[string]any{
					"node_name": node.NodeName,
				})
			}
		}
		state.Nodes[node.ID] = node
		store.AddAudit(state, s.ids, "node", node.ID, "enrolled", now, map[string]any{
			"node_name": node.NodeName,
			"region":    node.Region,
		})
		return nil
	})
	return node, err
}

// UpdateProfile applies a controlled profile update to a node looked up by its
// current name. Only nodes without an active (claimed) lease can be updated,
// and the caller must supply the node's current version. The node identifier
// never changes; quarantine can only be escalated from ready.
func (s *Service) UpdateProfile(ctx context.Context, input domain.NodeUpdateInput) (domain.ResolverNode, error) {
	if err := domain.ValidateNodeUpdateInput(input); err != nil {
		return domain.ResolverNode{}, err
	}
	input = domain.NormalizeNodeUpdateInput(input)
	now := s.clock.Now().UTC()
	var updated domain.ResolverNode
	err := s.store.Write(ctx, func(state *domain.State) error {
		domain.ExpireDueLeases(state, now)

		var node domain.ResolverNode
		found := false
		for _, candidate := range state.Nodes {
			if candidate.NodeName == input.NodeName {
				node = candidate
				found = true
				break
			}
		}
		if !found {
			return domain.NotFound("node", input.NodeName)
		}
		if node.Version != input.ExpectedVersion {
			return nodeVersionConflict(node, input.ExpectedVersion)
		}
		switch node.State {
		case domain.NodeLeased:
			return domain.InvalidState("leased node cannot be updated until its lease finishes", map[string]any{
				"node_id":   node.ID,
				"node_name": node.NodeName,
				"state":     node.State,
			})
		case domain.NodeQuarantined:
			return domain.InvalidState("quarantined node profile is locked and cannot be updated", map[string]any{
				"node_id":   node.ID,
				"node_name": node.NodeName,
				"state":     node.State,
			})
		}
		if node.State != domain.NodeReady {
			return domain.InvalidState("node cannot be updated in its current state", map[string]any{
				"node_id": node.ID,
				"state":   node.State,
			})
		}
		if domain.NodeHasActiveLease(state, node.ID) {
			return domain.InvalidState("node with an active lease cannot be updated", map[string]any{
				"node_id": node.ID,
				"state":   node.State,
			})
		}

		before := map[string]any{
			"node_name":   node.NodeName,
			"region":      node.Region,
			"state":       string(node.State),
			"quarantined": node.State == domain.NodeQuarantined,
		}
		changes := make(map[string]map[string]any)
		if node.Region != input.Region {
			changes["region"] = map[string]any{"before": node.Region, "after": input.Region}
			node.Region = input.Region
		}
		if input.Quarantine {
			changes["state"] = map[string]any{"before": string(domain.NodeReady), "after": string(domain.NodeQuarantined)}
			changes["quarantined"] = map[string]any{"before": false, "after": true}
			node.State = domain.NodeQuarantined
		}
		if len(changes) == 0 {
			updated = node
			return nil
		}
		node.Version++
		node.UpdatedAt = now
		after := map[string]any{
			"node_name":   node.NodeName,
			"region":      node.Region,
			"state":       string(node.State),
			"quarantined": node.State == domain.NodeQuarantined,
		}
		state.Nodes[node.ID] = node
		store.AddAudit(state, s.ids, "node", node.ID, "profile_updated", now, map[string]any{
			"node_name": node.NodeName,
			"before":    before,
			"after":     after,
			"changes":   changes,
			"version":   node.Version,
		})
		updated = node
		return nil
	})
	return updated, err
}

func nodeVersionConflict(node domain.ResolverNode, expected uint64) *domain.DomainError {
	return domain.NewError(domain.CodeVersionConflict, "node version does not match", map[string]any{
		"node_id":  node.ID,
		"expected": expected,
		"actual":   node.Version,
	})
}

func (s *Service) Get(ctx context.Context, nodeID string) (domain.ResolverNode, error) {
	var node domain.ResolverNode
	err := s.store.Read(ctx, func(state *domain.State) error {
		found, ok := state.Nodes[nodeID]
		if !ok {
			return domain.NotFound("node", nodeID)
		}
		node = found
		return nil
	})
	return node, err
}

func (s *Service) List(
	ctx context.Context,
	stateFilter domain.NodeState,
	region string,
	limit int,
	cursor string,
) (domain.NodePage, error) {
	if limit < 1 || limit > 100 {
		return domain.NodePage{}, domain.Validation("limit must be between 1 and 100", map[string]any{
			"limit": limit,
		})
	}
	after, err := platform.DecodeCursor(cursor)
	if err != nil {
		return domain.NodePage{}, domain.Validation("cursor is invalid", nil)
	}
	var page domain.NodePage
	err = s.store.Read(ctx, func(state *domain.State) error {
		items := make([]domain.ResolverNode, 0, len(state.Nodes))
		for _, node := range state.Nodes {
			if stateFilter != "" && node.State != stateFilter {
				continue
			}
			if region != "" && node.Region != region {
				continue
			}
			if after != "" && node.ID <= after {
				continue
			}
			items = append(items, node)
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].ID < items[j].ID
		})
		page.Meta.Limit = limit
		if len(items) > limit {
			page.Items = append(page.Items, items[:limit]...)
			page.Meta.NextCursor = platform.EncodeCursor(items[limit-1].ID)
			return nil
		}
		page.Items = append(page.Items, items...)
		return nil
	})
	return page, err
}

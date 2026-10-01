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

func (s *Service) Update(ctx context.Context, name string, input domain.NodeUpdateInput) (domain.ResolverNode, error) {
	if err := domain.ValidateNodeUpdateInput(input); err != nil {
		return domain.ResolverNode{}, err
	}
	input = domain.NormalizeNodeUpdateInput(input)
	nodeName := domain.NormalizeNodeName(name)
	now := s.clock.Now().UTC()
	var updated domain.ResolverNode
	err := s.store.Write(ctx, func(state *domain.State) error {
		var node domain.ResolverNode
		found := false
		for _, existing := range state.Nodes {
			if existing.NodeName == nodeName {
				node = existing
				found = true
				break
			}
		}
		if !found {
			return domain.NotFound("node", nodeName)
		}
		if node.Version != input.ExpectedVersion {
			return versionConflict(node, input.ExpectedVersion)
		}
		expiredLease := false
		for _, lease := range state.Leases {
			if lease.NodeID != node.ID || lease.State != domain.LeaseClaimed {
				continue
			}
			if lease.ExpiresAt.After(now) {
				return domain.InvalidState("node has an active lease", map[string]any{
					"node_name": node.NodeName,
					"node_id":   node.ID,
					"lease_id":  lease.ID,
					"state":     node.State,
				})
			}
			finishedAt := now
			lease.State = domain.LeaseExpired
			lease.FinishedAt = &finishedAt
			state.Leases[lease.ID] = lease
			if node.State == domain.NodeLeased {
				node.State = domain.NodeReady
				expiredLease = true
			}
		}
		changes := make(map[string]map[string]any)
		if input.Region != nil && *input.Region != node.Region {
			changes["region"] = map[string]any{"before": node.Region, "after": *input.Region}
			node.Region = *input.Region
		}
		if input.Quarantine != nil {
			target := domain.NodeReady
			if *input.Quarantine {
				target = domain.NodeQuarantined
			}
			if node.State != target {
				changes["state"] = map[string]any{"before": node.State, "after": target}
				node.State = target
			}
		}
		if len(changes) == 0 && !expiredLease {
			updated = node
			return nil
		}
		node.Version++
		node.UpdatedAt = now
		state.Nodes[node.ID] = node
		if len(changes) > 0 {
			store.AddAudit(state, s.ids, "node", node.ID, "updated", now, map[string]any{
				"node_name": node.NodeName,
				"changes":   changes,
			})
		}
		updated = node
		return nil
	})
	return updated, err
}

func versionConflict(node domain.ResolverNode, expected uint64) *domain.DomainError {
	return domain.NewError(domain.CodeVersionConflict, "node version does not match", map[string]any{
		"node_name": node.NodeName,
		"expected":  expected,
		"actual":    node.Version,
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

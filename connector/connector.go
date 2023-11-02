package connector

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type connector struct {
	tableConnectors []TableConnector
}

// NewConnector creates a new fivetran connector with the given tables and their respective connector
func NewConnector(tables []TableConnector) (Connector, error) {
	return &connector{
		tableConnectors: tables,
	}, nil
}

func (c *connector) Sync(ctx context.Context, req *Request) (*Response, error) {
	res := NewResponse()

	tableCount := len(c.tableConnectors)
	if tableCount == 0 {
		res.State = req.State
		res.HasMore = false
		return res, nil
	}

	tables := make([]*Table, tableCount)

	group, ctx := errgroup.WithContext(ctx)

	for i, t := range c.tableConnectors {
		idx := i
		tableConnector := t
		group.Go(func() error {
			table, err := syncTable(ctx, tableConnector, req.State, req.Secrets)
			if err != nil {
				return err
			}
			tables[idx] = table
			return nil
		})
	}

	err := group.Wait()
	if err != nil {
		return nil, err
	}

	hasMore := false
	for _, t := range tables {
		mergeTableResultsToResponse(res, t)
		hasMore = hasMore || t.HasMore
	}

	res.HasMore = hasMore

	return res, nil
}

func syncTable(ctx context.Context, t TableConnector, state, secrets map[string]string) (*Table, error) {
	cursor := getStateForTable(state, t.Name())
	return t.Sync(ctx, cursor, secrets)
}

func mergeTableResultsToResponse(res *Response, table *Table) {
	res.State[table.Name] = table.State
	if table.InsertRows != nil {
		res.Insert[table.Name] = table.InsertRows
	}
	if table.DeleteRows != nil {
		res.Delete[table.Name] = table.DeleteRows
	}
	res.Schema[table.Name] = &ResponseTableSchema{PrimaryKey: table.PrimaryKey}
}

func getStateForTable(state State, table string) string {
	k, ok := state[table]
	if !ok {
		return ""
	}
	return k
}

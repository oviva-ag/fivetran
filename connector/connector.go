package connector

import (
	"context"
)

type connector struct {
	tableConnectors []TableConnector
}

//NewConnector creates a new fivetran connector with the given tables and their respective connector
func NewConnector(tables []TableConnector) (Connector, error) {
	return &connector{
		tableConnectors: tables,
	}, nil
}

type syncResult struct {
	table *Table
	err   error
}

func (c *connector) Sync(ctx context.Context, req *Request) (*Response, error) {

	res := NewResponse()

	tableCount := len(c.tableConnectors)
	if tableCount == 0 {
		res.State = req.State
		res.HasMore = false
		return res, nil
	}

	tables := make(chan syncResult, tableCount)

	for _, t := range c.tableConnectors {
		go func() {
			table, err := syncTable(ctx, t, req.State, req.Secrets)
			tables <- syncResult{table: table, err: err}
		}()

	}

	hasMore := false
	for i := 0; i < tableCount; i++ {
		r := <-tables
		if r.err != nil {
			return nil, r.err
		}

		mergeTableResultsToResponse(res, r.table)
		hasMore = hasMore || r.table.HasMore
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

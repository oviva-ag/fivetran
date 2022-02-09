package connector

import (
	"context"
	_ "embed"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

//go:embed fixture_connector_sync_simple.json
var simpleSyncResponse string

type mockTableConnector struct {
	table *Table
}

func (m *mockTableConnector) Name() string {
	return m.table.Name
}

func (m *mockTableConnector) Sync(ctx context.Context, state string, secrets map[string]string) (*Table, error) {
	return m.table, nil
}

func TestConnector_Sync_empty(t *testing.T) {

	connector, err := NewConnector([]TableConnector{})
	assert.NoError(t, err)

	req := NewRequest()
	res, err := connector.Sync(context.TODO(), req)
	assert.NoError(t, err)

	assert.Equalf(t, false, res.HasMore, "has no more elements")

}

func TestConnector_Sync_simpleConnector(t *testing.T) {

	table := &Table{
		Name:       "mock",
		State:      "page:2",
		PrimaryKey: []string{"id"},
		InsertRows: []interface{}{
			struct {
				Id   string `json:"id"`
				Name string
			}{"1", "hello"},
			struct {
				Id   string `json:"id"`
				Name string
			}{"2", "dolly"},
		},
		DeleteRows: nil,
		HasMore:    false,
	}

	connector, err := NewConnector([]TableConnector{&mockTableConnector{
		table: table,
	}})
	assert.NoError(t, err)

	req := NewRequest()

	//when
	res, err := connector.Sync(context.TODO(), req)
	assert.NoError(t, err)

	j, _ := json.MarshalIndent(res, "", " ")
	assert.JSONEq(t, simpleSyncResponse, string(j), "equal response")
}

func TestConnector_Sync_hasMore(t *testing.T) {

	table := &Table{
		Name:       "mock",
		State:      "page:2",
		PrimaryKey: []string{"id"},
		InsertRows: nil,
		DeleteRows: nil,
		HasMore:    true,
	}

	connector, err := NewConnector([]TableConnector{&mockTableConnector{
		table: table,
	}})
	assert.NoError(t, err)

	req := NewRequest()

	//when
	res, err := connector.Sync(context.TODO(), req)
	assert.NoError(t, err)
	assert.Equal(t, true, res.HasMore, "has more")
}

func TestConnector_Sync_multipleConnectors(t *testing.T) {

	table1 := &Table{
		Name:       "table1",
		State:      "page:2",
		PrimaryKey: []string{"id"},
		InsertRows: []interface{}{
			struct {
				Id   string `json:"id"`
				Name string
			}{"1", "hello"},
			struct {
				Id   string `json:"id"`
				Name string
			}{"2", "dolly"},
		},
		DeleteRows: nil,
		HasMore:    false,
	}
	table1Connector := &mockTableConnector{table: table1}

	table2 := &Table{
		Name:       "table2",
		State:      "page:2",
		PrimaryKey: []string{"id"},
		InsertRows: []interface{}{
			struct {
				Id   string `json:"id"`
				Name string
			}{"X", "Hi"},
			struct {
				Id   string `json:"id"`
				Name string
			}{"Y", "Joe"},
		},
		DeleteRows: nil,
		HasMore:    false,
	}
	table2Connector := &mockTableConnector{table: table2}

	connector, err := NewConnector([]TableConnector{table1Connector, table2Connector})
	assert.NoError(t, err)

	req := NewRequest()

	//when
	res, err := connector.Sync(context.TODO(), req)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res.Insert)) // one entry for each table
}

package connector

import "context"

type (
	State   map[string]string
	Secrets map[string]string
)

// Request from Fivetran as describe here: https://fivetran.com/docs/functions#requestformat
type Request struct {
	Agent   string  `json:"agent"`
	State   State   `json:"state"`
	Secrets Secrets `json:"secrets"`
}

func NewRequest() *Request {
	return &Request{
		Agent:   "",
		State:   make(State),
		Secrets: make(Secrets),
	}
}

type (
	ResponseInserts map[string][]any
	ResponseDeletes map[string][]any
)

type ResponseTableSchema struct {
	PrimaryKey []string `json:"primary_key"`
}

// Response to Fivetran as described here https://fivetran.com/docs/functions#responseformat
type Response struct {
	State   State                           `json:"state"`
	Insert  ResponseInserts                 `json:"insert"`
	Delete  ResponseDeletes                 `json:"delete"`
	Schema  map[string]*ResponseTableSchema `json:"schema"`
	HasMore bool                            `json:"hasMore"`
}

func NewResponse() *Response {
	return &Response{
		State:   make(State),
		Insert:  make(ResponseInserts),
		Delete:  make(ResponseDeletes),
		Schema:  make(map[string]*ResponseTableSchema),
		HasMore: false,
	}
}

// Connector is translates Fivetran sync requests to actual data
// See also: https://fivetran.com/docs/functions#architecture
type Connector interface {
	// Sync is a request for a sync with Fivetran, the response should contain the (partial) sync
	Sync(ctx context.Context, req *Request) (*Response, error)
}

// Table represents a single synced table
type Table struct {
	// Name the name of the table
	Name string

	// State opaque string to represent the current 'state' of the sync, usually a database cursor
	State string

	// PrimaryKey the list of primary keys used for updates and deletes
	PrimaryKey []string

	// InsertRows additional rows to insert or update
	InsertRows []any

	// DeleteRows rows to mark deleted, providing just the id column suffices
	DeleteRows []any

	// HasMore whether the connector could provide more data or all is synced
	HasMore bool
}

// TableConnector is the connector for a single table
type TableConnector interface {
	// Name the name of the table
	Name() string

	// Sync synchronizes a table, starting from a previous state and returning the new data
	// as well as an updated state
	Sync(ctx context.Context, state string, secrets map[string]string) (*Table, error)
}

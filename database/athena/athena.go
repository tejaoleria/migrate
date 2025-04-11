package athena

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	nurl "net/url"
	"strings"

	"database/sql"

	_ "github.com/segmentio/go-athena"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/lib/pq"
	"go.uber.org/atomic"
)

func init() {
	// Register the Athena database driver with the migration package.
	// This allows the migration package to use the Athena driver for database migrations.
	database.Register("athena", &athenaAPI{})
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName    string
	MigrationsTable string
}

type athenaAPI struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	fb := &athenaAPI{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := fb.ensureVersionTable(); err != nil {
		return nil, err
	}

	return fb, nil
}

func (f *athenaAPI) Open(dsn string) (database.Driver, error) {
	purl, err := nurl.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// go-athena driver doesn't work with athena url scheme
	// so we need to replace it with the default one
	connStr := strings.Replace(migrate.FilterCustomQuery(purl).String(), "athena://", "", 1)
	db, err := sql.Open("athena", connStr)
	if err != nil {
		return nil, err
	}

	px, err := WithInstance(db, &Config{
		MigrationsTable: purl.Query().Get("x-migrations-table"),
		DatabaseName:    purl.Path,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (f *athenaAPI) Close() error {
	connErr := f.conn.Close()
	dbErr := f.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (f *athenaAPI) Lock() error {
	if !f.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (f *athenaAPI) Unlock() error {
	if !f.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (f *athenaAPI) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	// run migration
	query := string(migr[:])
	if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (f *athenaAPI) SetVersion(version int, dirty bool) error {

	// athena doesn't support transactions and hence it has to be done in two steps behind the lock

	oldVersion, _, err := f.Version()
	if err != nil {
		return database.Error{OrigErr: err, Err: "SetVersion failed"}
	}

	var query string
	if oldVersion == database.NilVersion {
		query = fmt.Sprintf(`INSERT INTO %v (version, dirty) VALUES (%d, %v)`,
			f.config.MigrationsTable,
			version,
			dirty)
	} else {
		query = fmt.Sprintf(`UPDATE %v SET version = %d, dirty = %v WHERE version = %d`,
			f.config.MigrationsTable,
			version,
			dirty,
			oldVersion)
	}

	if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
		return database.Error{OrigErr: err, Err: "SetVersion failed", Query: []byte(query)}
	}

	return nil
}

func (f *athenaAPI) Version() (int, bool, error) {
	query := `SELECT version, dirty FROM ` + f.config.MigrationsTable + ` LIMIT 1`
	var version int
	var dirty bool
	err := f.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		log.Printf("Version: %d, Dirty: %v", version, dirty)
		return version, dirty, nil
	}
}

func (f *athenaAPI) Drop() error {
	return errors.New("Drop not supported for athena driver as the underlying database can be of any type such as Iceberg, Hive, etc.")
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (f *athenaAPI) ensureVersionTable() error {
	if err := f.Lock(); err != nil {
		return err
	}

	defer f.Unlock()

	log.Printf("ensureVersionTable: %s", f.config.MigrationsTable)
	query := fmt.Sprintf(`DESCRIBE %v`, f.config.MigrationsTable)

	if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
		if strings.Contains(err.Error(), "EntityNotFoundException") {
			// create the table if it does not exist
			query = fmt.Sprintf(`CREATE TABLE %v (
     version BIGINT ,
     dirty BOOLEAN
    ) `, f.config.MigrationsTable)

			if _, err = f.conn.ExecContext(context.Background(), query); err == nil {
				// if the table creation fails, return the error
				return nil

			}

		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

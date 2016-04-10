package db_handler

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
	"crypto/md5"
	"sync"
	"errors"

	"../../../../packages/env_handler"
	"../../../../packages/errors/error_handler"
)

// DbHandler is using in db_object package
type DbHandler struct {
	dbh        *sql.DB
	envHandler *env_handler.EnvHandler
	Debug      bool
	DeleteNullValues bool
	dbURLHash string	
}

var dbh map[string]*sql.DB
var activeConnections map[string]uint16
var mainLocker *sync.RWMutex

func init() {
	dbh = make(map[string]*sql.DB)
	activeConnections = make(map[string]uint16)
	mainLocker = &sync.RWMutex{}
}

func New(envHandler *env_handler.EnvHandler) *DbHandler {
	h := &DbHandler{
		envHandler: envHandler,
	}
	return h
}

func (h *DbHandler) Connect(dbURL string) (err error) {
	dbURLHash := fmt.Sprintf("%x", md5.Sum([]byte(dbURL)))
	mainLocker.Lock()
	defer mainLocker.Unlock()
	if dbh[dbURLHash] == nil {
		if dbh[dbURLHash], err = sql.Open("postgres", dbURL); err != nil {
			return err
		}
	}
	h.dbURLHash = dbURLHash
	h.dbh = dbh[dbURLHash]
	if err = h.dbh.Ping(); err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbh.Ping: %s", err))
		// lets return err, in case we'll not panic in error_handler
		return err
	}
	activeConnections[h.dbURLHash]++
	return nil
}

func (h *DbHandler) checkConn() (err error) {
	mainLocker.RLock()
	defer mainLocker.RUnlock()
	if h.dbh == nil || dbh[h.dbURLHash] == nil {
		err = errors.New("connection closed")
		error_handler.Error(h.envHandler, err.Error())
		return err
	}
	return nil
}

func (h *DbHandler) Disconnect() (err error) {
	defer func() {
		h.dbh = nil
	}()
	if err = h.checkConn(); err != nil {
		error_handler.Error(h.envHandler, err.Error())
		return err
	}
	mainLocker.Lock()
	defer mainLocker.Unlock()
	if activeConnections[h.dbURLHash] > 0 {
		activeConnections[h.dbURLHash]--
	}
	if activeConnections[h.dbURLHash] > 0 {
		return nil
	}
	delete(activeConnections, h.dbURLHash)
	delete(dbh, h.dbURLHash)
	if err = h.dbh.Close(); err != nil {
		error_handler.Warning(h.envHandler, fmt.Sprintf("dbh.Close: %s", err))
		return err
	}
	return nil
}

func DisconnectAll(envHandler *env_handler.EnvHandler) {
	mainLocker.Lock()
	defer mainLocker.Unlock()
	for dbURLHash := range dbh {
		if dbh[dbURLHash] != nil {
			if err := dbh[dbURLHash].Close(); err != nil {
				error_handler.Warning(envHandler, fmt.Sprintf("dbh[\"%s\"].Close: %s", dbURLHash, err))
			}
			delete(activeConnections, dbURLHash)
			delete(dbh, dbURLHash)
		}
	}
}

func PreparePlaceholders(query string) string {
	if strings.Contains(query, "?") == false {
		return query
	}
	var newQuery string
	for i := 1; ; i++ {
		newQuery = strings.Replace(query, "?", fmt.Sprintf("$%d", i), 1)
		if newQuery == query {
			break
		}
		query = newQuery
	}
	return newQuery
}

func (h *DbHandler) Query(query string, args ...interface{}) (data []map[string]string, err error) {
	if err = h.checkConn(); err != nil {
		error_handler.Error(h.envHandler, err.Error())
		return nil, err
	}
	if h.Debug == true {
		error_handler.Debug(h.envHandler, fmt.Sprintf("dbh.Query: %s\n%#v", query, args))
	}
	rows, err := h.dbh.Query(query, args...)
	if err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbh.Query: %s\n%s\n%#v", err, query, args))
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbh.Query: %s", err))
		return nil, err
	}

	count := len(columns)
	values := make([]interface{}, count)
	valuesPtrs := make([]interface{}, count)
	columnsNames := make([]string, count)

	for i, col := range columns {
		valuesPtrs[i] = &values[i]
		columnsNames[i] = col
	}

	num := 0
	for rows.Next() {
		if err = rows.Scan(valuesPtrs...); err != nil {
			error_handler.Error(h.envHandler, fmt.Sprintf("rows.Scan: %s", err))
			return nil, err
		}

		data = append(data, make(map[string]string))

		for i := range values {
			b, ok := values[i].([]byte)
			if ok {
				data[num][columnsNames[i]] = string(b)
			} else {
				if values[i] == nil {
					if h.DeleteNullValues == false {
						data[num][columnsNames[i]] = ""
					}
				} else {
					data[num][columnsNames[i]] = fmt.Sprintf("%#v", values[i])
				}
			}
		}
		num++
	}

	if err = rows.Err(); err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbh.Query: %s", err))
		return nil, err
	}

	return data, nil
}

func (h *DbHandler) QueryRow(query string, args ...interface{}) (data map[string]string, err error) {
	dataRows, err := h.Query(query, args...)
	if err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("h.Query: %s", err))
		return nil, err
	}
	if len(dataRows) > 0 {
		data = dataRows[0]
	} else {
		data = make(map[string]string)
	}
	return data, nil
}

func (h *DbHandler) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	if err = h.checkConn(); err != nil {
		error_handler.Error(h.envHandler, err.Error())
		return nil, err
	}
	// looks like we can use sql.Result for RowsAffected() only,
	// for LastInsertId() use QueryRow with RETURNING
	if result, err = h.dbh.Exec(query, args...); err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("h.Exec: %s\n%s\n%#v", err, query, args))
		return nil, err
	}
	return result, nil
}

func (h *DbHandler) TruncateTables(tables []string) error {
	for i := range tables {
		query := fmt.Sprintf("TRUNCATE TABLE \"%s\" RESTART IDENTITY CASCADE", tables[i])
		if _, err := h.Exec(query); err != nil {
			error_handler.Error(h.envHandler, fmt.Sprintf("h.Exec: %s\n%s", err, query))
			return err
		}
	}
	return nil
}

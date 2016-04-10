package db_object

import (
	"errors"
	"fmt"

	"../../../../packages/db/postgresql/db_handler"
	"../../../../packages/env_handler"
	"../../../../packages/errors/error_handler"
)

type DbObject struct {
	envHandler      *env_handler.EnvHandler
	dbHandler       *db_handler.DbHandler
	fields          map[string]string
	tableName       string
	loaded          bool
	IdField         string
	changed         map[string]interface{}
	functions       map[string]string
	where           map[string]interface{}
	LoadAfterInsert bool
}

func New(envHandler *env_handler.EnvHandler, dbHandler *db_handler.DbHandler, tableName string) *DbObject {
	h := &DbObject{
		envHandler:      envHandler,
		dbHandler:       dbHandler,
		tableName:       tableName,
		IdField:         "id",
		fields:          make(map[string]string),
		changed:         make(map[string]interface{}),
		where:           make(map[string]interface{}),
		functions:       make(map[string]string),
		LoadAfterInsert: true,
	}
	return h
}

func (h *DbObject) Load(whereParams map[string]interface{}) (bool, error) {
	// be careful with "WHERE" that can include multiple rows
	// you can UPDATE or DELETE multiple rows (though loaded will be only one row)
	if len(whereParams) == 0 {
		err := errors.New(`No "WHERE" parameters passed.`)
		error_handler.Error(h.envHandler, fmt.Sprintf("%s", err))
		return false, err
	}
	var bind []interface{}
	where := ""
	i := uint(1)
	for key, value := range whereParams {
		if where != "" {
			where = where + " AND "
		}
		if string(key[0]) == "!" {
			// delete(whereParams, key)
			// key = string(key[1:])
			where = fmt.Sprintf("%s\"%s\" <> $%d", where, key[1:], i)
		} else {
			where = fmt.Sprintf("%s\"%s\" = $%d", where, key, i)
		}
		bind = append(bind, value)
		i++
	}
	query := `SELECT * FROM "` + h.tableName + `" WHERE ` + where
	var err error
	h.fields, err = h.dbHandler.QueryRow(query, bind...)
	if err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbHandler.QueryRow: %s", err))
		return false, err
	}
	// we need to clear changed fields and functions on each loading
	// because same db object can be used for many loading
	h.changed = make(map[string]interface{})
	h.functions = make(map[string]string)
	if len(h.fields) > 0 {
		h.loaded = true
	} else {
		h.loaded = false
	}
	h.where = whereParams
	return h.loaded, nil
}

func (h *DbObject) Loaded() bool {
	return h.loaded
}

func (h *DbObject) Get(fieldName string) string {
	return h.fields[fieldName]
}

func (h *DbObject) Set(fieldName string, fieldValue interface{}) {
	b, ok := fieldValue.([]byte)
	if ok {
		h.fields[fieldName] = string(b)
	} else {
		h.fields[fieldName] = fmt.Sprintf("%v", fieldValue)
	}
	h.changed[fieldName] = fieldValue
	delete(h.functions, fieldName)
}

func (h *DbObject) SetFunction(fieldName string, fieldFunction string) {
	h.functions[fieldName] = fieldFunction
	delete(h.fields, fieldName)
	delete(h.changed, fieldName)
}

func (h *DbObject) Save() (err error) {
	if h.loaded {
		return h.update()
	} else {
		return h.insert()
	}
}

func (h *DbObject) update() (err error) {
	var bind []interface{}
	i := uint(1)
	set := ""
	for key, value := range h.changed {
		if set != "" {
			set = set + ", "
		}
		set = fmt.Sprintf("%s\"%s\" = $%d", set, key, i)
		bind = append(bind, value)
		i++
	}

	for key, value := range h.functions {
		if set != "" {
			set = set + ", "
		}
		set = fmt.Sprintf("%s\"%s\" = %s", set, key, value)
	}

	where := ""
	for key, value := range h.where {
		if where != "" {
			where = where + " AND "
		}
		if string(key[0]) == "!" {
			where = fmt.Sprintf("%s\"%s\" <> $%d", where, key[1:], i)
		} else {
			where = fmt.Sprintf("%s\"%s\" = $%d", where, key, i)
		}
		// where = fmt.Sprintf("%s\"%s\" = $%d", where, key, i)
		bind = append(bind, value)
		i++
	}

	query := `UPDATE "` + h.tableName + `" SET ` + set + " WHERE " + where
	if _, err = h.dbHandler.Exec(query, bind...); err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbHandler.Exec: %s", err))
		return err
	}

	return nil
}

func (h *DbObject) insert() (err error) {
	var bind []interface{}
	i := uint(1)
	fields := ""
	values := ""
	// at first we need to include "where" parameters into the query
	// in case we'll call Save() right after Load()
	for key, value := range h.where {
		if string(key[0]) == "!" {
			continue
		}
		_, functionsKey := h.functions[key]
		_, changedKey := h.changed[key]
		if functionsKey == false && changedKey == false {
			if fields != "" {
				fields = fields + ", "
			}
			if values != "" {
				values = values + ", "
			}
			fields = fmt.Sprintf("%s\"%s\"", fields, key)
			values = fmt.Sprintf("%s$%d", values, i)
			bind = append(bind, value)
			i++
		}
	}
	for key, value := range h.functions {
		if fields != "" {
			fields = fields + ", "
		}
		if values != "" {
			values = values + ", "
		}
		fields = fmt.Sprintf("%s\"%s\"", fields, key)
		values = fmt.Sprintf("%s%s", values, value)
	}
	for key, value := range h.changed {
		if fields != "" {
			fields = fields + ", "
		}
		if values != "" {
			values = values + ", "
		}
		fields = fmt.Sprintf("%s\"%s\"", fields, key)
		values = fmt.Sprintf("%s$%d", values, i)
		bind = append(bind, value)
		i++
	}
	fields = " (" + fields + ")"
	values = " VALUES (" + values + ")"

	query := `INSERT INTO "` + h.tableName + `"` + fields + values
	if h.LoadAfterInsert {
		query = query + ` RETURNING "` + h.IdField + `"`
		var data map[string]string
		if data, err = h.dbHandler.QueryRow(query, bind...); err != nil {
			error_handler.Error(h.envHandler, fmt.Sprintf("dbHandler.QueryRow: %s", err))
			return err
		}
		if _, err = h.Load(map[string]interface{}{h.IdField: data[h.IdField]}); err != nil {
			error_handler.Error(h.envHandler, fmt.Sprintf("h.Load: %s", err))
			return err
		}
	} else {
		if _, err = h.dbHandler.Exec(query, bind...); err != nil {
			error_handler.Error(h.envHandler, fmt.Sprintf("dbHandler.Exec: %s", err))
			return err
		}
	}

	return nil
}

func (h *DbObject) Delete() error {
	if h.loaded == false {
		return nil
	}
	var bind []interface{}
	i := uint(1)
	where := ""
	for key, value := range h.where {
		if where != "" {
			where = where + " AND "
		}
		if string(key[0]) == "!" {
			where = fmt.Sprintf("%s\"%s\" <> $%d", where, key[1:], i)
		} else {
			where = fmt.Sprintf("%s\"%s\" = $%d", where, key, i)
		}
		bind = append(bind, value)
		i++
	}

	query := `DELETE FROM "` + h.tableName + `" WHERE ` + where
	if _, err := h.dbHandler.Exec(query, bind...); err != nil {
		error_handler.Error(h.envHandler, fmt.Sprintf("dbHandler.Exec: %s", err))
		return err
	}

	return nil
}

func (h *DbObject) Fields() map[string]string {
	return h.fields
}

package rethinker

import (
	r "gopkg.in/dancannon/gorethink.v1"
	"log"
)

type (
	RethinkConfig struct {
		r.ConnectOpts

		Debug bool
	}

	DBRun func(r.Term) r.Term
)

var (
	client *r.Session
)

func Startup(opts r.ConnectOpts) error {
	var err error

	client, err = r.Connect(opts)

	if err != nil {
		return err
	}

	return nil
}

func Session() *r.Session {
	if client == nil {
		log.Fatal("Rethink session has not been setup yet!")
	}
	return client
}

func Shutdown() error {
	return client.Close()
}

func Run(db, table string, run DBRun, optArgs ...r.RunOpts) (*r.Cursor, error) {
	tbl := r.DB(db).Table(table)
	term := run(tbl)
	return term.Run(Session(), optArgs...)
}

func RunWrite(db, table string, run DBRun, optArgs ...r.RunOpts) (r.WriteResponse, error) {
	tbl := r.DB(db).Table(table)
	term := run(tbl)
	return term.RunWrite(Session(), optArgs...)
}

func Table(db, table string) r.Term {
	return r.DB(db).Table(table)
}

func TableSetup(db, table string, optArgs ...r.TableCreateOpts) (r.WriteResponse, error) {
	wr, err := r.DBCreate(db).RunWrite(Session())
	if err != nil {
		return wr, err
	}

	return r.DB(db).TableCreate(table, optArgs...).RunWrite(Session())
}

func TableCreate(db, table string, optArgs ...r.TableCreateOpts) (r.WriteResponse, error) {
	return r.DB(db).TableCreate(table, optArgs...).RunWrite(Session())
}

func TableDrop(db, table string) (r.WriteResponse, error) {
	return r.DB(db).TableDrop(table).RunWrite(Session())
}

func DBDrop(db string) (r.WriteResponse, error) {
	return r.DBDrop(db).RunWrite(Session())
}

func EmptyTable(db, table string) (r.WriteResponse, error) {
	return RunWrite(db, table, func(table r.Term) r.Term {
		return table.Filter(r.Row.Field("id").Ne("")).Delete()
	})
}

func IndexCreate(db, table string, name interface{}, optArgs ...r.IndexCreateOpts) (r.WriteResponse, error) {
	return r.DB(db).Table(table).IndexCreate(name, optArgs...).RunWrite(Session())
}

//http://godoc.org/github.com/dancannon/gorethink#Term.Filter
func FindByFilter(db, table string, filter interface{}, rs interface{}, optArgs ...r.RunOpts) error {
	c, err := Run(db, table, func(table r.Term) r.Term {
		return table.Filter(filter)
	}, optArgs...)

	if err != nil {
		return err
	}

	return c.All(rs)
}

func FindAll(db, table string, rs interface{}, optArgs ...r.RunOpts) error {
	c, err := Run(db, table, func(table r.Term) r.Term {
		return table
	}, optArgs...)

	if err != nil {
		return err
	}

	return c.All(rs)
}

//http://godoc.org/github.com/dancannon/gorethink#Term.Get
func FindById(db, table string, id interface{}, rs interface{}, optArgs ...r.RunOpts) error {
	c, err := Run(db, table, func(table r.Term) r.Term {
		return table.Get(id)
	}, optArgs...)

	if err != nil {
		return err
	}

	return c.One(rs)
}

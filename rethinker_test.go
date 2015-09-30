package rethinker

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	r "gopkg.in/dancannon/gorethink.v1"
)

func TestDial(t *testing.T) {
	Convey("Given rethinkdb host, db name, table name", t, func() {
		host := "localhost:28015"
		dbName := "rethinker_test"
		tableName := "contacts"

		Printf("host[%s] db[%s] table[%s] \n", host, dbName, tableName)

		Convey("Startup connection", func() {
			var err error
			err = Startup(r.ConnectOpts{
				Address: host,
			})

			So(err, ShouldBeNil)

			Convey("Create db", func() {
				_, err = r.DBCreate(dbName).RunWrite(Session())
				So(err, ShouldBeNil)

				Convey("Create table", func() {
					_, err = r.DB(dbName).TableCreate(tableName).RunWrite(Session())
					So(err, ShouldBeNil)

					Convey("Insert item", func() {
						_, err := RunWrite(dbName, tableName, func(table r.Term) r.Term {
							return table.Insert(map[string]interface{}{
								"first_name": "tester",
							})
						})
						So(err, ShouldBeNil)
					})

					//					Convey("Find item", func() {
					//						c, err := Run(dbName, tableName, func(table r.Term) r.Term {
					//							return table.GetAllByIndex(0)
					//						})

					//						So(err, ShouldBeNil)

					//						item := make(map[string]interface{})
					//						c.One(&item)

					//						So(item["first_name"], ShouldEqual, "tester")
					//					})

					//					Convey("Delete item", func() {
					//						_, err := RunWrite(dbName, tableName, func(table r.Term) r.Term {
					//							return table.Delete()
					//						})
					//						So(err, ShouldBeNil)
					//					})

					Reset(func() {
						r.DB(dbName).TableDrop(tableName).RunWrite(Session())
					})
				})

			})

			Reset(func() {
				r.DBDrop(dbName).RunWrite(Session())
			})
		})
	})
}

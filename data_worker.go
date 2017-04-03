package softcache

type DataWorker interface {
	// it is Application's responsibility to maintenance the database connection pool.
	// When creating a DataWorker Object, please have the database connection object(for example: *xorm.Engine) as one of the member.

	Query(context string) (string, error)
}

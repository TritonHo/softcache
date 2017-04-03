package softcache

import "time"

type ResultSetType struct {
	//the duration that an execution lock should hold, so that no two thread access db simultaneously
	MaxExec time.Duration

	//the duration that an thread can wait for
	MaxWait time.Duration

	//HardTtl: after HardTtl ends, the cache will be deleted in redis
	//SoftTtl: after SoftTtl ends, the cache will still exists in the cache.
	HardTtl time.Duration
	SoftTtl time.Duration

	//the object that connect to the database and get back the data
	Worker DataWorker
}

//ttl is the ttl of the cache in redis
//This function return if this cache is still considered as "fresh"
func (rs ResultSetType) IsFresh(ttl time.Duration) bool {
	return rs.HardTtl-ttl < rs.SoftTtl
}

package softcache

import (
	"errors"
	"strings"
	"time"

	"github.com/TritonHo/simplelock"
	"github.com/go-redis/redis"
	cache "github.com/patrickmn/go-cache"
)

//the single thread lock duration
//this setting applies to manager internal locking only.
const lockDuration = 1 * time.Minute

var ErrWaitTooLong = errors.New(`The transaction is not proceeded due to long waiting.`)

type CacheManager struct {
	// the prefix for the lock name in item lock
	// i.e. the lockname would be lockNamePrefix + `-` + cacheId
	lockNamePrefix string

	// the lock name for updating the zset in redis,
	// the lock is a casual mechanism to avoid multiple CacheManager update he zset in the same time
	zsetLockName string

	// the name of redis sorted sort, to store the cacheId of cache that need to be refreshed
	zsetName string

	// the channel to communicate between different thread in the library
	taskChannel chan string

	// to avoid overwhelming the redis, registration of same redis-cache will be grouped locally
	recentRegistation *cache.Cache

	// functions to get the data from database,
	// it is function writer responsibility to marshal the dataset to string
	resultSetTypes map[string]ResultSetType

	//the lockManager to enforce global locking between multiple CacheManager
	lockManger *simplelock.LockManager

	//the redis connection pool object
	redisClient *redis.Client
}

func New(lockNamePrefix, zsetLockName, zsetName string, lockManager *simplelock.LockManager, redisConn *redis.Client) *CacheManager {
	cm := &CacheManager{
		lockNamePrefix:    lockNamePrefix,
		zsetLockName:      zsetLockName,
		zsetName:          zsetName,
		taskChannel:       make(chan string),
		recentRegistation: cache.New(lockDuration, lockDuration),
		resultSetTypes:    map[string]ResultSetType{},
		lockManger:        simplelock.New(redisConn),
		redisClient:       redisConn,
	}

	go cm.cacheRebuilder()
	go cm.taskPicker()

	return cm
}

func (cm *CacheManager) AddResultSetType(funcName string, rsType ResultSetType) error {
	if _, ok := cm.resultSetTypes[funcName]; ok {
		errors.New(`funcName has already existed.`)
	}
	cm.resultSetTypes[funcName] = rsType
	return nil
}

func getCacheId(funcName, context string) string {
	return funcName + `-` + context
}

func getFuncNameAndContext(cacheId string) (funcName string, context string) {
	temp := strings.SplitN(cacheId, `-`, 2)
	return temp[0], temp[1]
}

//if isHardRefresh is true, then the cache will be immediately deleted from the cache system
func (cm *CacheManager) Refresh(funcName, context string, isHardRefresh bool) error {
	rsType, ok := cm.resultSetTypes[funcName]
	if !ok {
		//not registered function, simply return and do nothing
		return errors.New(`funcName is not registered.`)
	}

	cacheId := getCacheId(funcName, context)
	if isHardRefresh {
		//step 1a: delete the cache directly
		if err := cm.redisClient.Del(cacheId).Err(); err != nil {
			return err
		}
	} else {

		//step 1b: shorten the cache TTL
		//otherwise the cache rebuilder may consider the cache is still fresh and refuse to rebuild it
		if ttl := rsType.HardTtl - rsType.SoftTtl - 10*time.Second; ttl > 0 {
			// elapsedTime > rs.SoftTtl
			// elapsedTime + ttl = rs.HardTtl
			// i.e. ttl < rs.HardTtl - rs.SoftTtl
			if err := cm.redisClient.Expire(cacheId, ttl).Err(); err != nil {
				return err
			}
		}
	}
	//step 2: get a lock, to protect the ZSet
	//no matter if the lock can be acquired, perform update.
	//as the race condition doesn't make disastrous result
	if isSuccessful, lockToken := cm.lockManger.GetLock(cm.zsetLockName, lockDuration, lockDuration); isSuccessful {
		defer cm.lockManger.ReleaseLock(cm.zsetLockName, lockToken)
	}

	//step 3: add the cacheId into redis
	return cm.redisClient.ZAdd(cm.zsetName, redis.Z{Score: float64(time.Now().Unix()), Member: cacheId}).Err()
}

func (cm *CacheManager) GetData(funcName, context string) (string, error) {
	rsType, ok := cm.resultSetTypes[funcName]
	if !ok {
		//not registered function, simply return and do nothing
		return ``, errors.New(`funcName is not registered.`)
	}

	cacheId := getCacheId(funcName, context)

	//do the 1st get cache
	if s, err := cm.redisClient.Get(cacheId).Result(); err == nil && s != `` {
		//We only need to register the Get event when there is cache-hit
		go cm.register(cacheId, rsType)

		return s, nil
	} else {
		if err != redis.Nil {
			return ``, err
		}
	}

	//cache miss, then try to get the lock
	itemLock := cm.lockNamePrefix + `-` + cacheId
	hasLock, lockToken := cm.lockManger.GetLock(itemLock, rsType.MaxExec, rsType.MaxWait)
	if !hasLock {
		return ``, ErrWaitTooLong
	}
	defer cm.lockManger.ReleaseLock(itemLock, lockToken)

	//do the 2st get cache
	//	There maybe another thread query the database and put the data into the redis
	// 	In order to avoid wasteful database query, we have to double check if the cache is not exists in redis.
	// 	Reminder: the database query is MUCH more expensive than the redis GET operation
	if s, err := cm.redisClient.Get(cacheId).Result(); err == nil && s != `` {
		//when there is cache-hit in the second Redis-Get, the cache should be just prepared by another thread.
		//thus no need to perform register
		return s, nil
	} else {
		if err != redis.Nil {
			return ``, err
		}
	}

	//no cache found, and thus load data from database
	s, err := rsType.Worker.Query(context)
	if err == nil {
		cm.redisClient.Set(cacheId, s, rsType.HardTtl)
	}
	return s, err
}

func (cm *CacheManager) register(cacheId string, rsType ResultSetType) {
	if _, found := cm.recentRegistation.Get(cacheId); found {
		//some thread in the same machine has already registered the event,
		//thus no need to do anything
		return
	}

	//get back the ttl from redis
	//remarks: in rare case, d may be = -2 second as the cache has expired, but it will not break the code and thus no need to take care
	d, err0 := cm.redisClient.TTL(cacheId).Result()
	if err0 != nil {
		panic(err0)
	}

	// elapsedTime + d = rs.HardTtl
	// thus, elapsedTime = rs.HardTtl - d

	//and we want to trigger the data query after SoftTtl passed:
	// elapsedTime + t = rs.SoftTtl
	// t = rs.SoftTtl - (rs.HardTtl - d)
	// and t is the time we have to schedule for

	t := rsType.SoftTtl - (rsType.HardTtl - d)

	//add 5 second, to avoid time inconsistancy between servers
	//when the refresh worker pick up this event, it must passed the SoftTtl
	t = t + (5 * time.Second)
	score := time.Now().Add(t).Unix()

	//add the cacheId into localSet
	if t > 0 {
		cm.recentRegistation.Add(cacheId, "DONOTCARE", t)
	}

	//get a lock, to protect the ZSet
	//no matter if the lock can be acquired, perform update.
	//as the race condition doesn't make disastrous result
	if isSuccessful, lockToken := cm.lockManger.GetLock(cm.zsetLockName, lockDuration, lockDuration); isSuccessful {
		defer cm.lockManger.ReleaseLock(cm.zsetLockName, lockToken)
	}

	//add the cacheId into redis
	cm.redisClient.ZAdd(cm.zsetName, redis.Z{Score: float64(score), Member: cacheId})
}

func (cm *CacheManager) pick() {
	//get a lock, to avoid multiple sync thread to sync the data
	if hasLock, lockToken := cm.lockManger.GetLock(cm.zsetLockName, lockDuration, 0); !hasLock {
		//cannot get the lock, skip current iteration
		return
	} else {
		defer cm.lockManger.ReleaseLock(cm.zsetLockName, lockToken)
	}

	score := float64(time.Now().Unix())

	candidates, err1 := cm.redisClient.ZRangeWithScores(cm.zsetName, 0, 10).Result()
	if err1 != nil {
		panic(err1)
	}

	for _, c := range candidates {
		if c.Score > score {
			break
		}
		//remove the current record from the set
		cacheId := c.Member.(string)
		cm.redisClient.ZRem(cm.zsetName, cacheId)

		//and then pass to the worker
		cm.taskChannel <- cacheId
	}
}

func (cm *CacheManager) taskPicker() {
	for {
		if len(cm.taskChannel) == 0 {
			//only pick task if there is no working one
			cm.pick()
		}

		//sleep randomly 1 second, to avoid overloading the redis server
		time.Sleep(1 * time.Second)
	}
}

func (cm *CacheManager) cacheRebuilder() {
	for {
		cacheId := <-cm.taskChannel
		funcName, context := getFuncNameAndContext(cacheId)

		rsType := cm.resultSetTypes[funcName]
		//get the TTL from redis. if the freshness is still within the SoftTtl, then do nothing
		//remarks: in rare case, d may be = -2 second as the cache has expired, but it will not break the code and thus no need to take care
		ttl, err0 := cm.redisClient.TTL(cacheId).Result()
		if err0 != nil {
			panic(err0)
		}

		if rsType.IsFresh(ttl) {
			//concurrency issue
			//we doesn't enforce strict locking for the cache refresh, thus likely other machine refreshed the cache.
			//we can simply ignore this event and then do nothing.
			continue
		}

		if s, err := rsType.Worker.Query(context); err == nil {
			cm.redisClient.Set(cacheId, s, rsType.HardTtl)
		}
	}
}

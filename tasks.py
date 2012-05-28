# -*- coding: utf-8 -*-
# Copyright (c) 2012, Scott Reynolds
# All rights reserved.

import redis
import math

from celery.task import task

VERSIONS = (1,)
REDIS_DB = 2
MAX_ACTIVITIES = 10

@task
def new_activity(user_id, timestamp, activity_type):
    """Record a new activity in all of user_id's follower's feeds"""
    activity = save_activity(user_id, timestamp, activity_type)

    for version in VERSIONS:
        aggr_key = write_aggr(activity, version)
        add_to_profile(activity['user_id'], aggr_key, activity, version)
        for follower in get_followers(activity['user_id']):
            add_to_feed(follower, aggr_key, activity, version)
            removed_keys = trim_activity_feed(follower, version)
            if removed_keys:
                garbage_collection(removed_keys)
        removed_profile_keys = trim_profile_feed(activity['user_id'], version)
        if removed_profile_keys:
            garbage_collection(removed_profile_keys)

@task
def follow_user(user_id, follower_id):
    """Follower_id follows user_id, update graph and back fill activities"""
    added = add_follower(user_id, follower_id)
    all_activities_key = "activities:%s" % (user_id,)

    if not added:
        return

    redis_connection = redis.StrictRedis(
        host='localhost', port=6379, db=REDIS_DB)

    retries = 3
    redis_connection.watch(all_activities_key)
    for x in xrange(retries):
        try:
            # Go through every activity
            for activity_key in redis_connection.smembers(all_activities_key):
                activity = redis_connection.hgetall(activity_key)
                activity['id'] = activity_key
                # write that activity to the feed at the right spot and cleanup.
                for version in VERSIONS:
                    aggr_key = write_aggr(activity, version)
                    add_to_feed(follower_id, aggr_key, activity, version)
                    removed_keys = trim_activity_feed(follower_id, version)

                    if removed_keys:
                        garbage_collection(removed_keys)
            # Go all the way here.
            break
        except redis.exceptions.WatchError:
            # Retry processing this guy.
            pass
@task
def unfollow(user_id, follower_id):
    """Follower_id unfollows user_id. Remove all activities"""
    retries = 10
    redis_connection = redis.StrictRedis(
        host='localhost', port=6379, db=REDIS_DB)

    follower_key = "followers:%s" % user_id
    redis_connection.srem(follower_key, follower_id)

    for version in VERSIONS:
        feed_key = "activity_feed:%s:%s" % (version, follower_id,)
        redis_connection.watch(feed_key)
        removed_keys = []
        for x in xrange(retries):
            try:
                entire_feed = redis_connection.zrange(feed_key, 0, MAX_ACTIVITIES)
                for key in entire_feed:
                    actor = int(key.split(":")[3])
                    if actor == user_id:
                        redis_connection.zrem(feed_key, key)
                        removed_keys.append(key)
                        break
            except redis.exceptions.WatchError:
                removed_keys = []

        redis_connection.unwatch()
        garbage_collection(removed_keys)

@task
def delete_activity(user_id, timestamp, activity_type):
    """Delete an activity from the aggregate and potentially remove aggregate
    from follower's feeds"""
    redis_connection = redis.StrictRedis(
        host='localhost', port=6379, db=REDIS_DB)
    activity_key = "activity:%s:%s:%s" % (user_id, timestamp, activity_type)
    all_activities_key = "activities:%s" % (user_id,)

    # Delete the activity and remove it from user's set.
    redis_connection.delete(activity_key)
    redis_connection.srem(all_activities_key, activity_key)

    day = math.floor(float(timestamp) / 86400)
    for version in VERSIONS:
        aggr_key = "activity_aggr:%s:%s:%s:%s" % (version, activity_type,
                                                  user_id, day,)
        redis_connection.srem(aggr_key, activity_key)
        size = redis_connection.scard(aggr_key)
        if not size:
            redis_connection.watch(aggr_key)
            try:
                redis_connection.delete(aggr_key + ":counter")

                # Remove from followers
                for feed_user_id in get_followers(user_id):
                    feed_key = "activity_feed:%s:%s" % (version, feed_user_id,)
                    redis_connection.zrem(feed_key, aggr_key)

                # Remove from profile feed.
                profile_feed_key = "activity_profile:%s:%s" % (version, user_id)
                redis_connection.zrem(profile_feed_key, aggr_key)
            except redis.exceptions.WatchError:
                # Someone else either deleted the aggr, or added a new item to it.
                pass
            redis_connection.unwatch()

def save_activity(user_id, timestamp, activity_type):
    """Saves the activity to the redis database"""
    redis_connection = redis.StrictRedis(
        host='localhost', port=6379, db=REDIS_DB)
    activity_key = "activity:%s:%s:%s" % (user_id, timestamp, activity_type)
    all_activities_key = "activities:%s" % (user_id,)
    mapping = dict(
        user_id=user_id,
        timestamp=int(math.floor(timestamp)),
        type=activity_type)

    redis_connection.hmset(activity_key, mapping)
    redis_connection.sadd(all_activities_key, activity_key)
    mapping['id'] = activity_key

    return mapping

def get_followers(user_id):
    """Return the set of followers for the user"""
    follower_key = "followers:%s" % user_id
    redis_connection = redis.StrictRedis(
        host='localhost', port=6379, db=REDIS_DB)
    return redis_connection.smembers(follower_key)

def add_follower(user_id, follower_id):
    """Sets follower_id to follow user_id"""
    follower_key = "followers:%s" % user_id
    redis_connection = redis.StrictRedis(
        host='localhost', port=6379, db=REDIS_DB)
    return redis_connection.sadd(follower_key, follower_id)

def write_aggr(activity, version):
    """Adds the activity to the aggregate that it belongs too and returns the
    key to that aggregate"""
    day = math.floor(float(activity['timestamp']) / 86400)
    aggr_key = "activity_aggr:%s:%s:%s:%s" % (version, activity['type'],
                                             activity['user_id'], day,)

    activity_id = encode_activity(activity)
    redis_connection = redis.StrictRedis(host='localhost', port=6379, db=REDIS_DB)
    redis_connection.sadd(aggr_key, activity_id)
    return aggr_key

def encode_activity(activity):
    """Encodes the activity into redis, storing only the ids of the objects
    involved in the activity that are not contained in the key"""
    return activity['id']

def add_to_profile(profile_id, aggr_key, activity, version):
    """Adds a reference to the activity aggregate to the actor's profile feed"""
    feed_key = "activity_profile:%s:%s" % (version, profile_id,)
    write_aggregate_to_feed(aggr_key, feed_key, activity['timestamp'])

def add_to_feed(feed_user_id, aggr_key, activity, version):
    """Adds a reference to the activity aggregate to the feed_user's activity
    feed"""
    feed_key = "activity_feed:%s:%s" % (version, feed_user_id,)
    write_aggregate_to_feed(aggr_key, feed_key, activity['timestamp'])

def write_aggregate_to_feed(aggr_key, feed_key, score):
    """Does the actual writing to redis for a zset feed."""
    retries = 5
    counter_key = aggr_key + ":counter"

    redis_connection = redis.StrictRedis(host='localhost', port=6379, db=REDIS_DB)
    redis_connection.watch(counter_key)

    for i in xrange(retries):
        try:
            added = redis_connection.zadd(feed_key, score, aggr_key)
            redis_connection.incr(counter_key, added)
            break
        except redis.exceptions.WatchError:
            pass
    redis_connection.unwatch()
        
def trim_activity_feed(feed_user_id, version):
    """Removes items for a activity feed when the feed gets too large."""
    feed_key = "activity_feed:%s:%s" % (version, feed_user_id,)
    return trim_feed(feed_key)

def trim_profile_feed(profile_id, version):
    """Removes items for a profile feed when the feed gets too large."""
    feed_key = "activity_profile:%s:%s" % (version, profile_id)
    return trim_feed(feed_key)

def trim_feed(feed_key):
    """Does remove the last few items from a user's feed that are lower then the
    max items. Returns the keys that were removed."""
    keys = []
    redis_connection = redis.StrictRedis(host='localhost', port=6379, db=REDIS_DB)

    try:
        redis_connection.watch(feed_key)
        # Gives plenty of range.
        keys = redis_connection.zrevrange(feed_key,
                                    MAX_ACTIVITIES,
                                    MAX_ACTIVITIES * 2)
        pipe = redis_connection.pipeline()

        for key in keys:
            counter_key = key + ":counter"
            pipe.zrem(feed_key, key)
            pipe.decr(counter_key, 1)
        pipe.execute()

    except redis.exceptions.WatchError:
        # This means someone else has added/remove stuff. So stay out of the
        # way.
        pass
    redis_connection.unwatch()
    return keys

def garbage_collection(keys):
    """Given a set of keys, check to see if those keys are present in any feeds
    and if not remove them from Redis."""
    redis_connection = redis.StrictRedis(host='localhost', port=6379, db=REDIS_DB)

    for key in keys:
        redis_connection.watch(key)
        try:
            counter_key = key + ":counter"
            feed_count = redis_connection.get(counter_key)
            if feed_count <= 0:
                pipe = redis_connection.pipeline()
                pipe.remove(key).remove(counter_key).execute()
        except redis.exceptions.WatchError:
            # This means that another activity was added to the aggregate and
            # perhaps it now has some feed referencing it. Or it means that
            # someone else has now removed it from redis. So let it be and move
            # on to the next key.
            pass

        redis_connection.unwatch()

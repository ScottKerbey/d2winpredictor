#!/usr/bin/env python
# coding: utf-8
# In[92]:
from pymongo import MongoClient
import json
import dota2api
import os
api_key = os.environ.get('DOTA2_API_KEY')

# %%
api = dota2api.Initialise(api_key)

# In[96]:
starting_match_id = 3867274477

# In[97]:
# ip = '20.40.184.204'
# ip = '104.209.80.106'
ip = "10.0.1.4"
username = os.environ.get('MONGO_USER')
pwd = os.environ.get('MONGO_PWD')
client = MongoClient(
    ip,
    27017,
    username=username,
    password=pwd,
    authMechanism='SCRAM-SHA-1'
    )

# In[6]:
# query = ("CREATE DATABASE dota7")
# cursor.execute(query)
db = client.dota

# In[7]:
# query = ("USE dota7")
# cursor.execute(query)

# In[9]:
# # Set up heroes table
# query = ("""
# CREATE TABLE `heroes` (
#   `id` int(10) unsigned NOT NULL,
#   `localized_name` varchar(50) DEFAULT NULL,
#   `name` varchar(50) DEFAULT NULL,
#   `url_small_portrait` varchar(255) DEFAULT NULL,
#   `matches_played` int(10) unsigned DEFAULT NULL,
#   `win_rate` float(5,4) DEFAULT NULL,
#   PRIMARY KEY (`id`)
# )""")
# cursor.execute(query)

# %%
# Populate heroes table
# heroes = api.get_heroes()
# hero_list = heroes['heroes']
# hero_stmt = """INSERT INTO heroes (id, localized_name, name, url_small_portrait) VALUES
#             ( %s, %s, %s, %s );"""
# for h in hero_list:
#     data = (h['id'],h['localized_name'],h['name'],h['url_small_portrait'])
#     cursor.execute(hero_stmt, data)

heroes = api.get_heroes()
hero_list = heroes['heroes']
heroes_table = db.heroes
for h in hero_list:
    key = {'_id': h['id']}
    data = {"$set": {
        'name': h['localized_name'],
        'portait': h['url_small_portrait']
    }}
    heroes_table.update_one(key, data, upsert=True)

# In[25]:
# Set up matches table
# query = ("""
# CREATE TABLE `matches` (
#   `match_id` int(10) unsigned NOT NULL,
#   `match_seq_num` int(10) unsigned NOT NULL,
#   `start_time` int(10) unsigned NOT NULL,
#   `duration` int(10) unsigned NOT NULL,
#   `radiant_win` tinyint(1) NOT NULL,
#   PRIMARY KEY (`match_id`)
# )""")
# cursor.execute(query)

# In[ ]:
# Set up match_hero table
# query = ("""
# CREATE TABLE `match_hero` (
#   `match_id` int(10) unsigned NOT NULL,
#   `player_slot` int(10) unsigned NOT NULL,
#   `hero_id` int(10) unsigned NOT NULL,
#   `hero_name` varchar(50) DEFAULT NULL,
#   PRIMARY KEY (`match_id`,`player_slot`)
# )""")
# cursor.execute(query)

# In[71]:
# Set up hero_matchups table
# query = ("""
# CREATE TABLE `hero_matchups` (
#   `hero_id` int(10) unsigned NOT NULL,
#   `opponent_id` int(10) unsigned NOT NULL,
#   `win_rate` float(5,4) DEFAULT NULL,
#   `matches_played` int(11) DEFAULT NULL,
#   PRIMARY KEY (`hero_id`,`opponent_id`)
# )""")
# cursor.execute(query)

# In[72]:
# Add a single match to the match and match_hero tables using match_id
# def add_match(match_id):
#     match = api.get_match_details(match_id=match_id)
#     exist_stmt = """SELECT * FROM matches WHERE match_id=%s"""
#     data = (match['match_id'],)
#     cursor.execute(exist_stmt, data)
#     result = cursor.fetchone();

#     match_stmt = """INSERT INTO matches (match_id, match_seq_num, start_time, duration, radiant_win) VALUES
#                 ( %s, %s, %s, %s, %s );"""
#     mh_stmt = """INSERT INTO match_hero (match_id, player_slot, hero_id, hero_name) VALUES
#                     ( %s, %s, %s, %s );"""

#     if result:
#         print("Match already exists in table")
#     else:
#         print("Adding match to table")
#         data = (match['match_id'], match['match_seq_num'], match['start_time'], match['duration'], match['radiant_win'])
#         cursor.execute(match_stmt, data)

#         for h in match['players']:
#             data = (match['match_id'], h['player_slot'], h['hero_id'], h['hero_name'])
#             cursor.execute(mh_stmt, data)
#     cnx.commit()


def add_match(match_id):
    match = api.get_match_details(match_id=match_id)
    matches_table = db.matches
    exist_query = {'_id': match['match_id']}
    result = matches_table.find_one(exist_query)
    # result = None

    match_hero_table = db.match_hero

    def match_insert(match_id, match_seq_num, start_time, duration,
                     radiant_win):
        return {
            '_id': match_id,
            'match_seq_num': match_seq_num,
            'start_time': start_time,
            'duration': duration,
            'radiant_win': radiant_win
        }

    def match_hero_insert(match_id, player_slot, hero_id, hero_name):
        return {
            '_id': int(str(match_id) + str(player_slot)),
            'match_id': match_id,
            'player_slot': player_slot,
            'hero_id': hero_id,
            'hero_name': hero_name
        }

    if result:
        print("Match already exists in table")
    else:
        print("Adding match to table")
        m_insert = match_insert(match['match_id'], match['match_seq_num'],
                                match['start_time'], match['duration'],
                                match['radiant_win'])
        matches_table.insert_one(m_insert)

        for h in match['players']:
            mh_insert = match_hero_insert(match['match_id'], h['player_slot'],
                                          h['hero_id'], h['hero_name'])
            match_hero_table.insert_one(mh_insert)


# In[30]:
add_match(starting_match_id)

# In[32]:
# Get the match_seq_num of the oldest match


def get_oldest_seq():
    matches_table = db.matches
    result = matches_table.find({}, {'match_seq_num': 1}).sort(
        'match_seq_num', 1).limit(1)
    return result[0]['match_seq_num']


# In[66]:
starting_match_seq = get_oldest_seq()


# In[73]:
# Get a batch of matches beggining at starting_seq number and fetching matches before
# def get_batch_matches(batch_size, starting_seq_num):
#     num_matches = 0
#     api_calls = 0

#     duplicate_count = 0
#     missing_seq_nums = 0
#     unsuitable_match = 0

#     exist_stmt = """SELECT * FROM matches WHERE match_id=%s"""
#     matches_stmt = """INSERT INTO matches (match_id, match_seq_num, start_time, duration, radiant_win) VALUES
#                     ( %s, %s, %s, %s, %s );"""
#     match_hero_stmt = """INSERT INTO match_hero (match_id, player_slot, hero_id) VALUES
#                         ( %s, %s, %s );"""

#     while num_matches < batch_size:

#         try:
#             api_calls = api_calls + 1
#             match_history = api.get_match_history_by_seq_num(
#                 start_at_match_seq_num=starting_seq_num)

#             for m in match_history['matches']:
#                 m = dota2api.src.parse.hero_id(m)

#                 if m['lobby_type'] != 0 or len(match['players']) < 10 or match['duration'] < 600:
#                     unsuitable_match = unsuitable_match + 1
#                 else:
#                     data = (m['match_id'],)
#                     cursor.execute(exist_stmt, data)
#                     result = cursor.fetchone()

#                     if result:
#                         duplicate_count = duplicate_count + 1
#                     else:
#                         num_matches = num_matches + 1
#                         data = (m['match_id'], m['match_seq_num'],
#                                 m['start_time'], m['duration'], m['radiant_win'])
#                         cursor.execute(matches_stmt, data)

#                         for h in m['players']:
#                             data = (m['match_id'],
#                                     h['player_slot'], h['hero_id'])
#                             cursor.execute(match_hero_stmt, data)
#         except json.JSONDecodeError:
#             missing_seq_nums = missing_seq_nums + 1

#         starting_seq_num = starting_seq_num - 200

#     cnx.commit()
#     print("Duplicates: " + str(duplicate_count))
#     print("Missing sequence numbers: " + str(missing_seq_nums))
#     print("Unsuitable matches: " + str(unsuitable_match))
#     print("Fetched " + str(num_matches) +
#           " suitable matches in " + str(api_calls) + " API calls.")


def get_batch_matches(batch_size, starting_seq_num):
    num_matches = 0
    api_calls = 0

    duplicate_count = 0
    missing_seq_nums = 0
    unsuitable_match = 0

    # match = api.get_match_details(match_id=match_id)
    matches_table = db.matches
    match_hero_table = db.match_hero
    # exist_query = {'_id': match['match_id']}
    # result = matches_table.find_one(exist_query)
    # exist_query = """SELECT * FROM matches WHERE match_id=%s"""

    def match_insert(match_id, match_seq_num, start_time, duration,
                     radiant_win):
        return {
            '_id': match_id,
            'match_seq_num': match_seq_num,
            'start_time': start_time,
            'duration': duration,
            'radiant_win': radiant_win
        }

    def match_hero_insert(match_id, player_slot, hero_id, hero_name):
        return {
            '_id': int(str(match_id) + str(player_slot)),
            'match_id': match_id,
            'player_slot': player_slot,
            'hero_id': hero_id,
            'hero_name': hero_name
        }

    # matches_stmt = """INSERT INTO matches (match_id, match_seq_num, start_time, duration, radiant_win) VALUES
    #                 ( %s, %s, %s, %s, %s );"""
    # match_hero_stmt = """INSERT INTO match_hero (match_id, player_slot, hero_id) VALUES
    #                     ( %s, %s, %s );"""

    while num_matches < batch_size:

        try:
            api_calls = api_calls + 1
            match_history = api.get_match_history_by_seq_num(
                start_at_match_seq_num=starting_seq_num)

            for m in match_history['matches']:
                m = dota2api.src.parse.hero_id(m)

                if m['lobby_type'] != 0 or len(m['players']) != 10 or m['duration'] < 600:
                    unsuitable_match = unsuitable_match + 1
                else:
                    exist_query = {'_id': m['match_id']}
                    result = matches_table.find_one(exist_query)

                    if result:
                        duplicate_count = duplicate_count + 1
                    else:
                        num_matches = num_matches + 1
                        m_insert = match_insert(m['match_id'],
                                                m['match_seq_num'],
                                                m['start_time'],
                                                m['duration'],
                                                m['radiant_win'])
                        matches_table.insert_one(m_insert)

                        for h in m['players']:
                            mh_insert = match_hero_insert(m['match_id'],
                                                          h['player_slot'],
                                                          h['hero_id'],
                                                          h['hero_name'])
                            match_hero_table.insert_one(mh_insert)
        except json.JSONDecodeError:
            missing_seq_nums = missing_seq_nums + 1

        starting_seq_num = starting_seq_num - 200

    print("Duplicates: " + str(duplicate_count))
    print("Missing sequence numbers: " + str(missing_seq_nums))
    print("Unsuitable matches: " + str(unsuitable_match))
    print("Fetched " + str(num_matches) +
          " suitable matches in " + str(api_calls) + " API calls.")


# In[68]:
get_batch_matches(200, starting_match_seq)

# %%
starting_match_seq = get_oldest_seq()
get_batch_matches(1000, starting_match_seq)

#%%

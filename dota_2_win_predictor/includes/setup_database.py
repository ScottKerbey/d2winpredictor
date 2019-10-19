# %%
from pymongo import MongoClient
import json
import dota2api
import os
api_key = os.environ.get('DOTA2_API_KEY')

# %%
api = dota2api.Initialise(api_key)

# %%
starting_match_id = 3867274477

# %%
ip = "10.0.1.4"
port = 27017
username = os.environ.get('MONGO_USER')
pwd = os.environ.get('MONGO_PWD')
client = MongoClient(
    ip,
    port,
    username=username,
    password=pwd,
    authMechanism='SCRAM-SHA-1'
    )

# %%
db = client.dota

# %%
# Populate heroes table
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

# %%
# Add a single match to the match and match_hero tables using match_id
def add_match(match_id):
    match = api.get_match_details(match_id=match_id)
    matches_table = db.matches
    exist_query = {'_id': match['match_id']}
    result = matches_table.find_one(exist_query)

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


# %%
add_match(starting_match_id)

# %%
# Get the match_seq_num of the oldest match
def get_oldest_seq():
    matches_table = db.matches
    result = matches_table.find({}, {'match_seq_num': 1}).sort(
        'match_seq_num', 1).limit(1)
    return result[0]['match_seq_num']

# %%
starting_match_seq = get_oldest_seq()

# %%
# Get a batch of matches beggining at starting_seq number and fetching matches before
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

# %%
get_batch_matches(200, starting_match_seq)

# %%
starting_match_seq = get_oldest_seq()
get_batch_matches(1000, starting_match_seq)

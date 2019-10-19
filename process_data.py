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

# In[97]:
# ip = '20.40.184.204'
ip = '104.209.80.106'
client = MongoClient(ip, 27017)

# In[6]:
# query = ("CREATE DATABASE dota7")
# cursor.execute(query)
db = client.dota

# In[ ]:
# def process_heroes():
#     query = ("SELECT * FROM heroes")
#     cursor.execute(query)
#     global hero_win_rates
#     hero_win_rates = []
#     h_columns = cursor.column_names
#     heroes = cursor.fetchall()
#     for h in heroes:
#         h = dict(zip(h_columns, h))
#         hero_win_rates.append({'id': h['id'], 'name': h['localized_name'], 'matches_won': 0.0, 'matches_played': 0.0})
#     match_hero_stmt = ("SELECT * FROM match_hero WHERE hero_id=%s")
#     for h in hero_win_rates:
#         h['hero_matchup'] = []
#         for he in heroes:
#             he = dict(zip(h_columns, he))
#             h['hero_matchup'].append({'id': he['id'], 'name': he['localized_name'], 'matches_won_against': 0.0, 'matches_played_against': 0.0})
#         data = (h['id'],)
#         cursor.execute(match_hero_stmt,data)
#         mh_columns = cursor.column_names
#         for mh in cursor.fetchall():
#             mh = dict(zip(mh_columns, mh))

#             matches_stmt = ("SELECT * FROM matches WHERE match_id=%s")
#             data = (mh['match_id'],)
#             cursor.execute(matches_stmt,data)

#             mat = dict(zip(cursor.column_names, cursor.fetchone()))
#             if ((mh['player_slot'] < 128 and mat['radiant_win']) or (mh['player_slot'] >= 128 and not mat['radiant_win'])):
#                 h['matches_won'] = h['matches_won'] + 1
#             h['matches_played'] = h['matches_played'] + 1

#             match_hero_stmt2 = ("SELECT * FROM match_hero WHERE match_id=%s AND NOT hero_id=%s")
#             data = (mh['match_id'],mh['hero_id'])
#             cursor.execute(match_hero_stmt2,data)

#             for mh2 in cursor.fetchall():
#                 mh2 = dict(zip(mh_columns, mh2))
#                 pl = next((pl for pl in h['hero_matchup'] if pl['id']==mh2['hero_id']), None)
#                 if(pl):
#                     if(mh['player_slot']<128 and mh2['player_slot']>=128 or mh['player_slot']>=128 and mh2['player_slot']<128):
#                         pl['matches_played_against'] += 1
#                         if(mh['player_slot']<128 and mat['radiant_win'] or mh['player_slot']>=128 and not mat['radiant_win']):
#                             pl['matches_won_against'] += 1
#     for h in hero_win_rates:
#         h['win_rate'] = None if (h['matches_played']==0) else h['matches_won']/h['matches_played']
#         save_wr_stmt = ("""UPDATE heroes
#                             SET win_rate = %s,
#                                 matches_played = %s
#                             WHERE id = %s""")
#         data = (None if (h['win_rate']==None) else h['win_rate'], h['matches_played'],h['id'])
#         cursor.execute(save_wr_stmt,data)
#         for pl in h['hero_matchup']:
#             pl['win_rate'] = None if (pl['matches_played_against']==0) else pl['matches_won_against']/pl['matches_played_against']
#             save_mwr_stmt = ("""UPDATE hero_matchups
#                                 SET win_rate = %s,
#                                     matches_played = %s
#                                 WHERE hero_id = %s AND opponent_id = %s""")
#             data = (None if not pl['win_rate'] else pl['win_rate'], pl['matches_played_against'],h['id'],pl['id'])
#             cursor.execute(save_mwr_stmt,data)
#         print("id: {}, name: {}, wins: {}, total: {}, win rate: {}".format(h['id'],
                                                                        #    h['name'],
                                                                        #    h['matches_won'],
def process_heroes():
    heroes_table = db.heroes
    match_hero_table = db.match_hero
    matches_table = db.matches
    hero_matchups_table = db.hero_matchups

    global hero_win_rates
    hero_win_rates = []

    heroes = heroes_table.find()
    heroes = list(heroes)
    for h in heroes:
        hero_win_rates.append({'_id': h['_id'], 'name': h['name'], 'matches_won': 0.0, 'matches_played': 0.0})

    def match_hero_query(hero_id):
        return {'hero_id': hero_id}

    for h in hero_win_rates:
        h['hero_matchup'] = []
        for he in heroes:
            h['hero_matchup'].append({'hero_id': he['_id'], 'name': he['name'], 'matches_won_against': 0.0, 'matches_played_against': 0.0})
        mh_query = match_hero_query(h['_id'])
        mh_result = match_hero_table.find(mh_query)
        if mh_result:
            for mh in mh_result:

                def matches_query(match_id):
                    return {'_id': match_id}

                m_query = matches_query(mh['match_id'])
                m_result = matches_table.find_one(m_query)

                if ((mh['player_slot'] < 128 and m_result['radiant_win']) or (mh['player_slot'] >= 128 and not m_result['radiant_win'])):
                    h['matches_won'] = h['matches_won'] + 1
                h['matches_played'] = h['matches_played'] + 1

                def match_hero_query2(match_id, hero_id):
                    return {'match_id': match_id, 'hero_id': {'$ne': hero_id}}

                mh_query2 = match_hero_query2(mh['match_id'], mh['hero_id'])
                mh_result2 = match_hero_table.find(mh_query2)

                for mh2 in mh_result2:
                    pl = next((pl for pl in h['hero_matchup'] if pl['hero_id']==mh2['hero_id']), None)
                    if(pl):
                        if(mh['player_slot']<128 and mh2['player_slot']>=128 or mh['player_slot']>=128 and mh2['player_slot']<128):
                            pl['matches_played_against'] += 1
                            if(mh['player_slot']<128 and m_result['radiant_win'] or mh['player_slot']>=128 and not m_result['radiant_win']):
                                pl['matches_won_against'] += 1

    # hero_matchups_table
    # for h in hero_win_rates:
    #     h['win_rate'] = None if (h['matches_played']==0) else h['matches_won']/h['matches_played']
    #     key = {'_id': h['_id']}
    #     data = {"$set": {
    #         'win_rate': h['win_rate'],
    #         'matches_played': h['matches_played']
    #     }}
    #     heroes_table.update_one(key, data)
    #     for pl in h['hero_matchup']:
    #         pl['win_rate'] = None if (pl['matches_played_against']==0) else pl['matches_won_against']/pl['matches_played_against']
    #         key = {
    #             '_id': str(h['_id']) + '_' + str(pl['hero_id']),
    #             'hero_id': h['_id'],
    #             'opponent_id': pl['hero_id']
    #         }
    #         data = {"$set": {
    #             'win_rate': pl['win_rate'],
    #             'matches_won_against': pl['matches_won_against'],
    #             'matches_played_against': pl['matches_played_against']
    #         }}
    #         hero_matchups_table.update_one(key, data, upsert=True)
    #     print("id: {}, name: {}, wins: {}, total: {}, win rate: {}".format(h['_id'], h['name'], h['matches_won'], h['matches_played'], "-" if (h['win_rate']==None) else '%.2f' % (100*h['win_rate'])+"%"))

    # in heroes table instead
    for h in hero_win_rates:
        h['win_rate'] = None if (h['matches_played']==0) else h['matches_won']/h['matches_played']
        key = {'_id': h['_id']}
        data = {"$set": {
            'win_rate': h['win_rate'],
            'matches_played': h['matches_played']
        }}
        heroes_table.update_one(key, data)
        for pl in h['hero_matchup']:
            pl['win_rate'] = None if (pl['matches_played_against']==0) else pl['matches_won_against']/pl['matches_played_against']

            # try update
            key = {'_id': h['_id'], "matchups.opponent_id": pl['hero_id']}
            data = {"$set": {
                'matchups.$.win_rate': pl['win_rate'],
                'matchups.$.matches_won_against': pl['matches_won_against'],
                'matchups.$.matches_played_against': pl['matches_played_against']
            }}
            result = heroes_table.update_one(key, data)

            # else push
            if result.modified_count < 1:
                key = {'_id': h['_id']}
                data = {"$push": {'matchups': {
                    "opponent_id": pl['hero_id'],
                    'win_rate': pl['win_rate'],
                    'matches_won_against': pl['matches_won_against'],
                    'matches_played_against': pl['matches_played_against']
                }}}
                heroes_table.update_one(key, data)
        print("id: {}, name: {}, wins: {}, total: {}, win rate: {}".format(h['_id'], h['name'], h['matches_won'], h['matches_played'], "-" if (h['win_rate']==None) else '%.2f' % (100*h['win_rate'])+"%"))


# In[ ]:
# def generate_hero_matchups():
#     query = ("DELETE FROM hero_matchups")
#     cursor.execute(query)
#     query = ("SELECT id FROM heroes")
#     cursor.execute(query)
#     create_mwr_stmt = ("""INSERT INTO hero_matchups (hero_id, opponent_id) VALUES
#                                     (%s, %s)""")
#     hid_list = cursor.fetchall()
#     for hid1 in hid_list:
#         hid1 = hid1[0]  # hero_id
#         for hid2 in hid_list:
#             hid2 = hid2[0]  # hero_id
#             if hid1 != hid2:
#                 data = (hid1, hid2)
#                 cursor.execute(create_mwr_stmt, data)

# In[ ]:
process_heroes()

# In[ ]:
# def suggest_1():
#     global hero_win_rates
#     top_heroes = []
#     max_top = 5
#     sorted_hero_win_rates = sorted(hero_win_rates, key=lambda k: (-1,-1) if (k['win_rate']==None) else (k['win_rate'],k['matches_played']), reverse=True)
#     temp_wr = sorted_hero_win_rates[0]['win_rate']
#     i = 0
#     while i < max_top or sorted_hero_win_rates[i]['win_rate'] == temp_wr:
#         temp_wr = sorted_hero_win_rates[i]['win_rate']
#         add_hero = {'id': sorted_hero_win_rates[i]['id'],
#                     'matches_played': sorted_hero_win_rates[i]['matches_played'],
#                     'matches_won': sorted_hero_win_rates[i]['matches_won'],
#                     'name': sorted_hero_win_rates[i]['name'],
#                     'win_rate': sorted_hero_win_rates[i]['win_rate']}
#         top_heroes.append(add_hero)
#         print(add_hero)
#         i = i + 1
#     return top_heroes


def suggest_1():
    global hero_win_rates
    top_heroes = []
    max_top = 5
    sorted_hero_win_rates = sorted(hero_win_rates, key=lambda k: (-1,-1) if (k['win_rate']==None) else (k['win_rate'],k['matches_played']), reverse=True)
    temp_wr = sorted_hero_win_rates[0]['win_rate']
    i = 0
    while i < max_top or sorted_hero_win_rates[i]['win_rate'] == temp_wr:
        temp_wr = sorted_hero_win_rates[i]['win_rate']
        add_hero = {'_id': sorted_hero_win_rates[i]['_id'],
                    'matches_played': sorted_hero_win_rates[i]['matches_played'],
                    'matches_won': sorted_hero_win_rates[i]['matches_won'],
                    'name': sorted_hero_win_rates[i]['name'],
                    'win_rate': sorted_hero_win_rates[i]['win_rate']}
        top_heroes.append(add_hero)
        print(add_hero)
        i = i + 1
    return top_heroes

# In[ ]:
def suggest_2(hero_id1):
    global hero_win_rates
    top_heroes = []
    max_top = 5
    h = next((h for h in hero_win_rates if h['_id']==hero_id1), None)
    print({'_id': h['_id'],
           'matches_played': h['matches_played'],
           'matches_won': h['matches_won'],
           'name': h['name'],
           'win_rate': h['win_rate']})
    print("suggestions:")
    #     sorted_hero_matchups = sorted(h['hero_matchup'], key=lambda k: (2,2) if (k['win_rate']==None) else (k['win_rate'],k['matches_played_against']), reverse=False) 
    sorted_hero_matchups = sorted(h['hero_matchup'], key=lambda k: -1 if (k['win_rate']==None) else k['matches_played_against'], reverse=True)
    sorted_hero_matchups = sorted(sorted_hero_matchups, key=lambda k: 2 if (k['win_rate']==None) else k['win_rate'], reverse=False)
    temp_wr = sorted_hero_matchups[0]['win_rate']
    temp_m = sorted_hero_matchups[0]['matches_played_against']
    i = 0
    while i < max_top or (sorted_hero_matchups[i]['win_rate'] >= temp_wr and sorted_hero_matchups[i]['matches_played_against'] >= temp_m):
        temp_wr = sorted_hero_matchups[i]['win_rate']
        temp_m = sorted_hero_matchups[i]['matches_played_against']
        add_hero = {'hero_id': sorted_hero_matchups[i]['hero_id'],
                    'matches_played_against': sorted_hero_matchups[i]['matches_played_against'],
                    'matches_won_against': sorted_hero_matchups[i]['matches_won_against'],
                    'name': sorted_hero_matchups[i]['name'],
                    'win_rate': sorted_hero_matchups[i]['win_rate']}
        top_heroes.append(add_hero)
        print(add_hero)
        # print(sorted_hero_matchups[i])
        i = i + 1
    return top_heroes

#%%

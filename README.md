# dota_2_win_predictor
Predicting which team will win based on the heroes picked.

Dependencies:
Tensorflow, Numpy, pymongo and dota2api

dota2api is no longer available on pypi repository.
To install it download it from github, build and install it yourself.

Requires a MongoDB server to run.

Set your dota API key as an environment variable to be imported.
You can get a key from here: https://steamcommunity.com/dev/apikey

Setup environment variables for your username and password for connecting to the database

Run setup_database.py in a jupyter notebook to set up database tables and populate them with data

Run win_classifier.py with --train flag to train the neural network
You can provide other flags to tweak the operation of the training

Run win_classifier.py with --predict to run a prediction of a match
e.g. >python win_classifier.py --predict="1,2,3,4,5,6,7,8,9,10" 
with the first 5 numbers being the hero_ids of the radiant heroes and the second 5 dire.
The output will be the probability that radiant will win.

#!/bin/bash
DATEVAR=`date +\%Y-\%m-\%d_\%H:\%M:\%S`
echo $DATEVAR
source /home/tono/anaconda3/bin/activate IconProject
python /home/tono/ICONProject/twitter/daily_tweet_bot.py

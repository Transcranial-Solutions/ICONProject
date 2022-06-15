#!/bin/bash
/usr/bin/keychain /home/tono/.ssh/id_ed25519
#source /home/tono/.keychain/Tono-RB-sh

. ~/.keychain/$HOSTNAME-sh

source /home/tono/anaconda3/bin/activate IconProject
cd /home/tono/ICONProject
git pull



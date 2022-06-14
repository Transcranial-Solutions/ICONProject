#######!/bin/bash
#!/home/tono/anaconda3/envs/IconProject/bin/python
### auto push git ###
cd /home/tono/ICONProject
git pull
git add .
git commit -m "Automatic save commit initiated at $(date)"
git push origin master 

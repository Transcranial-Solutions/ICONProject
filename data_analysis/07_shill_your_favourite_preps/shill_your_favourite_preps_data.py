
#########################################################################
## Project: Shill Your Favourite P-Reps                                ##
## Date: November 2020                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


import os
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import seaborn as sns
import numpy as np
from PIL import Image

currPath = os.getcwd()
inPath = os.path.join(currPath, "07_shill_your_favourite_preps")
maskPath = os.path.join(inPath, "mask")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# data
shill_prep_data = pd.read_csv(os.path.join(inPath, 'shill_your_preps.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# barplot
sns.set(style="darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(8, 6))
ax = sns.countplot(x="Preps",
                   data=shill_prep_data,
                   order=shill_prep_data['Preps'].value_counts().index,
                   edgecolor="white",
                   palette=sns.cubehelix_palette(len(shill_prep_data), start=.5, rot=-.75))
ax.grid(False)
ax.set_xlabel('P-Reps', fontsize=12, labelpad=10, weight='bold')
ax.set_ylabel('Mentions (count)', fontsize=12, labelpad=10, weight='bold')
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")


ax.set_title("Number of P-Reps mentioned \n for 'Shill Your Favourite P-Reps'", fontsize=14, weight='bold')
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.tight_layout()


# saving
plt.savefig(os.path.join(inPath, 'P-Reps_mentions.png'))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# P-Reps
prep = shill_prep_data[['Preps']]
prep = prep['Preps'].str.replace('\n', '')
prep = prep.values

# Reasons
text = shill_prep_data.drop_duplicates(['Reasons'])[['Reasons']]
text = text['Reasons'].str.replace('\n', '').str.replace('-', '')
text = text.values

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# reasons for each prep
def wc_prep(df, prep):
    text = df[shill_prep_data['Preps'] == prep]
    text = text.drop_duplicates(['Reasons'])[['Reasons']]
    text = text['Reasons'].str.replace('\n', '').str.replace('-', '')
    text = text.values
    return(text)

def plot_cloud(wordcloud):
    # matplotlib.rcParams["figure.dpi"] = 100
    # Set figure size
    # plt.figure(figsize=(9, 6))
    plt.figure(figsize=(16, 8))
    # Display image
    plt.imshow(wordcloud)
    # No axis details
    plt.axis("off")

# Generate word cloud
def gen_wc(text):
    wc = WordCloud(width=1600, height=800,
              random_state=1, background_color='black',
              colormap='Set2', collocations=False,
              stopwords=STOPWORDS).generate(str(text))
    return(wc)

# Plot
# preps
wordcloud_preps = gen_wc(prep)
plot_cloud(wordcloud_preps)
plt.savefig(os.path.join(inPath, 'wc_preps.png'), facecolor='k', bbox_inches='tight')

# reasons
wordcloud_reasons = gen_wc(text)
plot_cloud(wordcloud_reasons)
plt.savefig(os.path.join(inPath, 'wc_reasons.png'), facecolor='k', bbox_inches='tight')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#



unique_prep = shill_prep_data.drop_duplicates(['Preps'])[['Preps']]

# loop around P-Reps
for k in range(len(unique_prep)):

    # prep name
    prep_name = unique_prep['Preps'].iloc[k].replace('@', '')

    # prep reasons
    prep_texts = wc_prep(shill_prep_data, unique_prep['Preps'].iloc[k])

    # wordcloud and save
    wordcloud_preps_text = gen_wc(prep_texts)
    plot_cloud(wordcloud_preps_text)
    plt.savefig(os.path.join(inPath, 'wc_' + prep_name + '.png'), facecolor='k', bbox_inches='tight')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#




## bad quality
def transform_format(val):
    if val == 0:
        return 255
    else:
        return val

mask = np.array(Image.open(os.path.join(maskPath, 'icon.png')))

# Transform your mask into a new one that will work with the function:
transformed_mask = np.ndarray((mask.shape[0],mask.shape[1]), np.int32)
for i in range(len(mask)):
    transformed_mask[i] = list(map(transform_format, mask[i]))

# Generate wordcloud
wordcloud_reasons = WordCloud(width=800, height=800,
                      random_state=1, background_color='black',
                      colormap='Set2', collocations=False,
                      stopwords=STOPWORDS, mask=transformed_mask).generate(str(text))

# Plot
icx = plot_cloud(wordcloud_reasons)
plt.savefig(os.path.join(inPath, 'icx_reasons.png'), facecolor='k', bbox_inches='tight')

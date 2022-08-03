
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
    # plt.figure(figsize=(12, 4))
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


# test = ["a", "about", "above", "across", "after", "again", "against", "ain", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "an", "and", "another", "any", "anybody", "anyone", "anything", "anywhere", "are", "area", "areas", "aren", "around", "as", "ask", "asked", "asking", "asks", "at", "away", "b", "back", "backed", "backing", "backs", "be", "became", "because", "become", "becomes", "been", "before", "began", "behind", "being", "beings", "below", "best", "better", "between", "big", "both", "but", "by", "c", "came", "can", "cannot", "case", "cases", "certain", "certainly", "clear", "clearly", "come", "could", "couldn", "d", "did", "didn", "differ", "different", "differently", "do", "does", "doesn", "doing", "don", "done", "down", "downed", "downing", "downs", "during", "e", "each", "early", "either", "end", "ended", "ending", "ends", "enough", "even", "evenly", "ever", "every", "everybody", "everyone", "everything", "everywhere", "f", "face", "faces", "fact", "facts", "far", "felt", "few", "find", "finds", "first", "for", "four", "from", "full", "fully", "further", "furthered", "furthering", "furthers", "g", "gave", "general", "generally", "get", "gets", "give", "given", "gives", "go", "going", "good", "goods", "got", "great", "greater", "greatest", "group", "grouped", "grouping", "groups", "h", "had", "hadn", "has", "hasn", "have", "haven", "having", "he", "her", "here", "hers", "herself", "high", "higher", "highest", "him", "himself", "his", "how", "however", "i", "if", "important", "in", "interest", "interested", "interesting", "interests", "into", "is", "isn", "it", "its", "itself", "j", "just", "k", "keep", "keeps", "kind", "knew", "know", "known", "knows", "l", "large", "largely", "last", "later", "latest", "least", "less", "let", "lets", "like", "likely", "ll", "long", "longer", "longest", "m", "ma", "made", "make", "making", "man", "many", "may", "me", "member", "members", "men", "might", "mightn", "more", "most", "mostly", "mr", "mrs", "much", "must", "mustn", "my", "myself", "n", "necessary", "need", "needed", "needing", "needn", "needs", "never", "new", "newer", "newest", "next", "no", "nobody", "non", "noone", "nor", "not", "nothing", "now", "nowhere", "number", "numbers", "o", "of", "off", "often", "old", "older", "oldest", "on", "once", "one", "only", "open", "opened", "opening", "opens", "or", "order", "ordered", "ordering", "orders", "other", "others", "our", "ours", "ourselves", "out", "over", "own", "p", "part", "parted", "parting", "parts", "per", "perhaps", "place", "places", "point", "pointed", "pointing", "points", "possible", "present", "presented", "presenting", "presents", "problem", "problems", "put", "puts", "q", "quite", "r", "rather", "re", "really", "right", "room", "rooms", "s", "said", "same", "saw", "say", "says", "second", "seconds", "see", "seem", "seemed", "seeming", "seems", "sees", "several", "shall", "shan", "she", "should", "shouldn", "show", "showed", "showing", "shows", "side", "sides", "since", "small", "smaller", "smallest", "so", "some", "somebody", "someone", "something", "somewhere", "state", "states", "still", "such", "sure", "t", "take", "taken", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "therefore", "these", "they", "thing", "things", "think", "thinks", "this", "those", "though", "thought", "thoughts", "three", "through", "thus", "to", "today", "together", "too", "took", "toward", "turn", "turned", "turning", "turns", "two", "u", "under", "until", "up", "upon", "us", "use", "used", "uses", "v", "ve", "very", "w", "want", "wanted", "wanting", "wants", "was", "wasn", "way", "ways", "we", "well", "wells", "went", "were", "weren", "what", "when", "where", "whether", "which", "while", "who", "whole", "whom", "whose", "why", "will", "with", "within", "without", "won", "work", "worked", "working", "works", "would", "wouldn", "x", "y", "year", "years", "yet", "you", "ya", "young", "younger", "youngest", "your", "yours", "yourself", "yourselves", "z"];
# curr = "fuck,fuk,pissed,youre,lol,haha,also,poop,i,me,my,myself, we,us,our,ours,ourselves,you,your,yours,yourself,yourselves,he,him,his,himself,she,her,hers,herself,it,its,itself,they,them,their,theirs,themselves,what,which,who,whom,whose,this,that,these,those,am,is,are,was,were,be,been,being,have,has,had,having,do,does,did,doing,will,would,should,can,could,ought,i'm,you're,he's,she's,it's,we're,they're,i've,you've,we've,they've,i'd,you'd,he'd,she'd,we'd,they'd,i'll,you'll,he'll,she'll,we'll,they'll,isn't,aren't,wasn't,weren't,hasn't,haven't,hadn't,doesn't,don't,didn't,won't,wouldn't,shan't,shouldn't,can't,cannot,couldn't,mustn't,let's,that's,who's,what's,here's,there's,when's,where's,why's,how's,a,an,the,and,but,if,or,because,as,until,while,of,at,by,across,almost,alone,already,although,always,among,for,with,about,against,between,into,through,during,before,after,above,another,below,to,from,up,upon,down,in,out,on,off,over,under,again,further,then,once,here,there,when,where,why,how,all,any,anyone,anything,anywhere,area,areas,both,each,few,more,most,other,some,such,no,nor,not,only,own,same,so,than,around,ask,asked,asking,away,back,backed,backing,too,very,become,becomes,became,begin,began,begun,say,says,said,shall";
# wow = ["'ll","'tis","'twas","'ve","10","39","a","a's","able","ableabout","about","above","abroad","abst","accordance","according","accordingly","across","act","actually","ad","added","adj","adopted","ae","af","affected","affecting","affects","after","afterwards","ag","again","against","ago","ah","ahead","ai","ain't","aint","al","all","allow","allows","almost","alone","along","alongside","already","also","although","always","am","amid","amidst","among","amongst","amoungst","amount","an","and","announce","another","any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere","ao","apart","apparently","appear","appreciate","appropriate","approximately","aq","ar","are","area","areas","aren","aren't","arent","arise","around","arpa","as","aside","ask","asked","asking","asks","associated","at","au","auth","available","aw","away","awfully","az","b","ba","back","backed","backing","backs","backward","backwards","bb","bd","be","became","because","become","becomes","becoming","been","before","beforehand","began","begin","beginning","beginnings","begins","behind","being","beings","believe","below","beside","besides","best","better","between","beyond","bf","bg","bh","bi","big","bill","billion","biol","bj","bm","bn","bo","both","bottom","br","brief","briefly","bs","bt","but","buy","bv","bw","by","bz","c","c'mon","c's","ca","call","came","can","can't","cannot","cant","caption","case","cases","cause","causes","cc","cd","certain","certainly","cf","cg","ch","changes","ci","ck","cl","clear","clearly","click","cm","cmon","cn","co","co.","com","come","comes","computer","con","concerning","consequently","consider","considering","contain","containing","contains","copy","corresponding","could","could've","couldn","couldn't","couldnt","course","cr","cry","cs","cu","currently","cv","cx","cy","cz","d","dare","daren't","darent","date","de","dear","definitely","describe","described","despite","detail","did","didn","didn't","didnt","differ","different","differently","directly","dj","dk","dm","do","does","doesn","doesn't","doesnt","doing","don","don't","done","dont","doubtful","down","downed","downing","downs","downwards","due","during","dz","e","each","early","ec","ed","edu","ee","effect","eg","eh","eight","eighty","either","eleven","else","elsewhere","empty","end","ended","ending","ends","enough","entirely","er","es","especially","et","et-al","etc","even","evenly","ever","evermore","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","face","faces","fact","facts","fairly","far","farther","felt","few","fewer","ff","fi","fifteen","fifth","fifty","fify","fill","find","finds","fire","first","five","fix","fj","fk","fm","fo","followed","following","follows","for","forever","former","formerly","forth","forty","forward","found","four","fr","free","from","front","full","fully","further","furthered","furthering","furthermore","furthers","fx","g","ga","gave","gb","gd","ge","general","generally","get","gets","getting","gf","gg","gh","gi","give","given","gives","giving","gl","gm","gmt","gn","go","goes","going","gone","good","goods","got","gotten","gov","gp","gq","gr","great","greater","greatest","greetings","group","grouped","grouping","groups","gs","gt","gu","gw","gy","h","had","hadn't","hadnt","half","happens","hardly","has","hasn","hasn't","hasnt","have","haven","haven't","havent","having","he","he'd","he'll","he's","hed","hell","hello","help","hence","her","here","here's","hereafter","hereby","herein","heres","hereupon","hers","herself","herse”","hes","hi","hid","high","higher","highest","him","himself","himse”","his","hither","hk","hm","hn","home","homepage","hopefully","how","how'd","how'll","how's","howbeit","however","hr","ht","htm","html","http","hu","hundred","i","i'd","i'll","i'm","i've","i.e.","id","ie","if","ignored","ii","il","ill","im","immediate","immediately","importance","important","in","inasmuch","inc","inc.","indeed","index","indicate","indicated","indicates","information","inner","inside","insofar","instead","int","interest","interested","interesting","interests","into","invention","inward","io","iq","ir","is","isn","isn't","isnt","it","it'd","it'll","it's","itd","itll","its","itself","itse”","ive","j","je","jm","jo","join","jp","just","k","ke","keep","keeps","kept","keys","kg","kh","ki","kind","km","kn","knew","know","known","knows","kp","kr","kw","ky","kz","l","la","large","largely","last","lately","later","latest","latter","latterly","lb","lc","least","length","less","lest","let","let's","lets","li","like","liked","likely","likewise","line","little","lk","ll","long","longer","longest","look","looking","looks","low","lower","lr","ls","lt","ltd","lu","lv","ly","m","ma","made","mainly","make","makes","making","man","many","may","maybe","mayn't","maynt","mc","md","me","mean","means","meantime","meanwhile","member","members","men","merely","mg","mh","microsoft","might","might've","mightn't","mightnt","mil","mill","million","mine","minus","miss","mk","ml","mm","mn","mo","more","moreover","most","mostly","move","mp","mq","mr","mrs","ms","msie","mt","mu","much","mug","must","must've","mustn't","mustnt","mv","mw","mx","my","myself","myse”","mz","n","na","name","namely","nay","nc","nd","ne","near","nearly","necessarily","necessary","need","needed","needing","needn't","neednt","needs","neither","net","netscape","never","neverf","neverless","nevertheless","new","newer","newest","next","nf","ng","ni","nine","ninety","nl","no","no-one","nobody","non","none","nonetheless","noone","nor","normally","nos","not","noted","nothing","notwithstanding","novel","now","nowhere","np","nr","nu","null","number","numbers","nz","o","obtain","obtained","obviously","of","off","often","oh","ok","okay","old","older","oldest","om","omitted","on","once","one","one's","ones","only","onto","open","opened","opening","opens","opposite","or","ord","order","ordered","ordering","orders","org","other","others","otherwise","ought","oughtn't","oughtnt","our","ours","ourselves","out","outside","over","overall","owing","own","p","pa","page","pages","part","parted","particular","particularly","parting","parts","past","pe","per","perhaps","pf","pg","ph","pk","pl","place","placed","places","please","plus","pm","pmid","pn","point","pointed","pointing","points","poorly","possible","possibly","potentially","pp","pr","predominantly","present","presented","presenting","presents","presumably","previously","primarily","probably","problem","problems","promptly","proud","provided","provides","pt","put","puts","pw","py","q","qa","que","quickly","quite","qv","r","ran","rather","rd","re","readily","really","reasonably","recent","recently","ref","refs","regarding","regardless","regards","related","relatively","research","reserved","respectively","resulted","resulting","results","right","ring","ro","room","rooms","round","ru","run","rw","s","sa","said","same","saw","say","saying","says","sb","sc","sd","se","sec","second","secondly","seconds","section","see","seeing","seem","seemed","seeming","seems","seen","sees","self","selves","sensible","sent","serious","seriously","seven","seventy","several","sg","sh","shall","shan't","shant","she","she'd","she'll","she's","shed","shell","shes","should","should've","shouldn","shouldn't","shouldnt","show","showed","showing","shown","showns","shows","si","side","sides","significant","significantly","similar","similarly","since","sincere","site","six","sixty","sj","sk","sl","slightly","sm","small","smaller","smallest","sn","so","some","somebody","someday","somehow","someone","somethan","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify","specifying","sr","st","state","states","still","stop","strongly","su","sub","substantially","successfully","such","sufficiently","suggest","sup","sure","sv","sy","system","sz","t","t's","take","taken","taking","tc","td","tell","ten","tends","test","text","tf","tg","th","than","thank","thanks","thanx","that","that'll","that's","that've","thatll","thats","thatve","the","their","theirs","them","themselves","then","thence","there","there'd","there'll","there're","there's","there've","thereafter","thereby","thered","therefore","therein","therell","thereof","therere","theres","thereto","thereupon","thereve","these","they","they'd","they'll","they're","they've","theyd","theyll","theyre","theyve","thick","thin","thing","things","think","thinks","third","thirty","this","thorough","thoroughly","those","thou","though","thoughh","thought","thoughts","thousand","three","throug","through","throughout","thru","thus","til","till","tip","tis","tj","tk","tm","tn","to","today","together","too","took","top","toward","towards","tp","tr","tried","tries","trillion","truly","try","trying","ts","tt","turn","turned","turning","turns","tv","tw","twas","twelve","twenty","twice","two","tz","u","ua","ug","uk","um","un","under","underneath","undoing","unfortunately","unless","unlike","unlikely","until","unto","up","upon","ups","upwards","us","use","used","useful","usefully","usefulness","uses","using","usually","uucp","uy","uz","v","va","value","various","vc","ve","versus","very","vg","vi","via","viz","vn","vol","vols","vs","vu","w","want","wanted","wanting","wants","was","wasn","wasn't","wasnt","way","ways","we","we'd","we'll","we're","we've","web","webpage","website","wed","welcome","well","wells","went","were","weren","weren't","werent","weve","wf","what","what'd","what'll","what's","what've","whatever","whatll","whats","whatve","when","when'd","when'll","when's","whence","whenever","where","where'd","where'll","where's","whereafter","whereas","whereby","wherein","wheres","whereupon","wherever","whether","which","whichever","while","whilst","whim","whither","who","who'd","who'll","who's","whod","whoever","whole","wholl","whom","whomever","whos","whose","why","why'd","why'll","why's","widely","width","will","willing","wish","with","within","without","won","won't","wonder","wont","words","work","worked","working","works","world","would","would've","wouldn","wouldn't","wouldnt","ws","www","x","y","ye","year","years","yes","yet","you","you'd","you'll","you're","you've","youd","youll","young","younger","youngest","your","youre","yours","yourself","yourselves","youve","yt","yu","z","za","zero","zm","zr"]
#
# my_list = curr.split(",")
# fin = test + list(set(my_list) - set(test))
# fin = fin + list(set(wow) - set(fin))
#
# all_comments = ",".join(map(str, fin))
# all_comments = ' '.join(all_comments.split())


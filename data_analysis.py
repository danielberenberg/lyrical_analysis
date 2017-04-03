# -*- coding: utf-8 -*-
"""
Created on Thu Dec  8 22:16:27 2016

@author: danberenberg

 * ------------------------------------------------------- *
 |CS/STAT 287 FINAL PROJECT: DATA ANALYSIS FILE            |
 |                                                         |
 |This file is intended to analyze the data compiles in    | 
 |data_xtraction.py by computing the yule coefficients     |
 |and syllabic averages of each genre and decade.          |
 |                                                         |
 |This file provides an interface to compute large batches |
 |of yule coefficiets and syllabic averages, as well as    |
 |organizing the data set by genre or production year.     |
 |                                                         |
 |In addition to the above mentioned interface, there is   |
 |also a plotting function to observe the syllabic average |
 |changes over the course of time.                         |
 * ------------------------------------------------------- *
nltk resources:

[http://stackoverflow.com/questions/5876040/number-of-syllables-for-words-in-a-text]
[http://www.nltk.org/genindex.html#S]
[http://stackoverflow.com/questions/14541303/count-the-number-of-syllables-in-a-word]
[http://matplotlib.org/users/legend_guide.html]
"""

#Necessary imports
from nltk.corpus import cmudict
import sqlite3
import matplotlib.pyplot as plt
import random
import numpy as np

cmu = cmudict.dict() #globalize the cmu

try: #open the database
    conn = sqlite3.connect('song_records.db')
    curs    = conn.cursor()
    print("Connection Successful")

except:
    print("Could not connect to sqlite3 db...")


def sanitize(corpora_word_dict):
    """
    Filter out nonsensical words and characters that are left from the 
    extraction and filtering.
    """    
    
    #get the genres/years 
    corpora_keys = [k for k in corpora_word_dict.keys()]
    
    #a list of lists of all words
    all_words_list  = [corpora_word_dict[k] for k in corpora_keys]
    
    #remove repeats from all_words_list
    all_words = [set(w for d in all_words_list for w in [w for w in d.keys()])]
    
    #create the actual word set oobject
    all_words_ = set()
    
    #for each corpus word set, union it with all_words_
    for set_ in all_words:
        all_words_ = all_words_.union(set_)
    
    #voila! a list of all unique words in the data set
    all_words_ = list(all_words_)
    
    # single out words that are less than 4 and greater than 16 characters.
    #after scanning through the data set, it is notable that below and above these
    #benchmarks respectively are words that have no context meaning.
    too_small = [w for w in all_words_ if len(w) < 4]
    too_big = [w for w in all_words_ if len(w) > 16]
    
    #filter out bigs and smalls
    all_words_ = [w for w in all_words_ if (w not in too_small and w not in too_big)]
    
    #now conform the working corpora_word_dict to the word set of all_words_
    for corpus_key in corpora_keys:
       
       corpus = corpora_word_dict[corpus_key]
       corpus_words = [key for key in corpus.keys()]
       
       for word in corpus_words:
           
           if word not in all_words_:
               corpus.pop(word)

def stratify_by_genre():
    """
    Group the data by genre. Put each data point in its genre list and return 
    the dictionary of lists for each genre. This is the 'strat_dict' mentioned in
    other functions.    
    """     
    #grab all the data
    curs.execute("SELECT song_name, artist_name, lyrics, genre, year FROM media")
    all_data = curs.fetchall()
    
    #organize each    
    row_list = []    
    for data in all_data:
        row_dict = {'song':data[0],'artist':data[1],'lyrics':data[2],'genre':data[3]}
        row_list.append(row_dict)
    
    #empty dictionary for each corpus
    the_genre_dict = {'Rock':[],'Jazz':[],'Country':[],'Pop':[],'Dance':[],
                      'Hip-Hop':[],'R&B/Soul':[],'Latin':[],'Christian/Gospel':[]}
    
    #populate dictionary
    for row in row_list:
        genre = row['genre']
        the_genre_dict[genre].append(row)
    
    return the_genre_dict #return the finished word dictionary.

def stratify_by_year(choice):
    """
    Group the data by year/decade (depending on the choice variable). 
    Put each data point in its year/decade list and return the dictionary 
    of lists for each genre. This is the 'strat_dict' mentioned in 
    other functions.  
    
    """
    
    #grab all the data
    curs.execute("SELECT song_name, artist_name, lyrics, genre, year FROM media")
    all_data = curs.fetchall()
    
    #organize each    
    row_list = []    
    for data in all_data:
        row_dict = {'song':data[0],'artist':data[1],'lyrics':data[2],'genre':data[3],
                    'year':data[4]}
        row_list.append(row_dict)
    
    #individual year dictionary
    if choice == 'year':
        the_year_dict = {str(year):[] for year in range(1980,2010+1)}
        
        for row in row_list:
            year = row['year']
            the_year_dict[year].append(row)
    
    #decade dictionary
    if choice == 'decade':
        the_year_dict = {'1980-1989':[],
                         '1990-1999':[],
                         '2000-2010':[]}
            
        for row in row_list:
        
            year = int(row['year'])
        
            if year >=1980 and year <=1989:
                the_year_dict['1980-1989'].append(row)
            elif year >=1990 and year <=1999:
                the_year_dict['1990-1999'].append(row)
            elif year >=2000 and year <=2010:
                the_year_dict['2000-2010'].append(row)
            
    return the_year_dict #return it regardless


###############################################################################
############################ COMPUTE YULES ####################################
############################################################################### 
def count_word_occurrence(strat_dict):   
    """
    Count the word occurrence of a each word in an already stratified dictionary.
    Return the count dictionary.
    
    Inputs:
        -strat_dict ---> A dictionary that has the data stratified by year or genre
    
    Output:
        -Word Occurrence counts for every word in the strat_dict corpuses
    
    """    
    #iterable keys
    keys = [key for key in strat_dict.keys()]
    
    #empty word counts
    corpora_word_counts = {}
    
    for key in keys: #iterate over keys
        corpus = strat_dict[key]  #crack open one corpus
        corpus_word_set = list()  #make an empty list
        
        for row in corpus: #iterate over data in that corpus
            lyric_set = list(set(row['lyrics'].split()))  #split it into words
            corpus_word_set = corpus_word_set + lyric_set #add it to the big dictionary
        
        corpus_word_dict = {w:0 for w in corpus_word_set} #empty counts
        
        for row in corpus: #populate counter
            for word in row['lyrics'].split():
                corpus_word_dict[word] +=1
        
        corpora_word_counts[key] = corpus_word_dict
    
    return corpora_word_counts #return the counts

def yule(corpus1_dict,corpus2_dict):
    """

    Compute the Yule coefficients for two corpora against eachother and return the 
    sorted list of mini lists containing the top words simultaneously common
    and rare words.
    
    """
    
    #get each key for each corpus (should be words that hash to counts)
    c1k = set(k for k in corpus1_dict.keys())
    c2k = set(k for k in corpus2_dict.keys())
    
    #get the intersection of keys
    list_set = list(c1k.intersection(c2k))
    
    yuleCoefList = [] #empty Yule list
    
    #for eachword in the intersection
    for word in list_set:
        
        #get the count
        wordCt1 = corpus1_dict[word]
        wordCt2 = corpus2_dict[word]
        
        #compute the yule
        yule = ((wordCt1 - wordCt2)/(wordCt1 + wordCt2))
        
        #add the word, yule to list with yule leading (for sorting purposes)
        mini = [yule,word]
        
        #add to the Yule list
        yuleCoefList.append(mini)
    
    #sort so that data are arranged in Yule-wise increasing order.
    yuleCoefList.sort()
    
    #return the finished list
    return yuleCoefList    

def yule_batch(corpora_word_dict):
    """
    Compute the yule coefficient of a single corpus against the entire data set
    minus that corpus for each corpus in the corpora_word_dict.
    
    """
    
    keys = [key for key in corpora_word_dict.keys()] #crack open keys
    yule_batch = {} #make an empty yule batch dictionary
    
    for key in keys: 
        corpus = corpora_word_dict[key]       
    
        #create word count dict for all words in corpora_word_dict not including corpus
        master_word_dict = filter_out_key(key,corpora_word_dict)
        
        #measure Yules of corpus against entire dataset
        yules = yule(master_word_dict,corpus)
        yule_batch[key] = yules #add that yule list to associate it with a corpus.
        
        
    return yule_batch #return the entire batch!

def write_yules_to_file(yule_batch):
    """
    Write all the top (and bottom) 100 Yule coefficients from a previously
    calculated Yule batch to a file.
    """
    
    #crack open keys
    yule_batch_keys = [k for k in yule_batch.keys()]
    
    #iterate over each Yule list    
    for key in yule_batch_keys:
        
        #surrounded by try statement because during development
        #there were corpora with < 200 words in their word set. 
        #these corpora were eventually combined with larger corpora.
        try: 
            #grab a Yule list
            yules = yule_batch[key]
            
            #get the most unique to that corpus
            top100_left  = yules[:100]
            
            #least common in that corpus
            top100_right = yules[:-101:-1]
            
            #formatting stuff
            right_widths = [len(mini[1]) for mini in top100_right]           
            mx_r = max(right_widths)            
            
            #Avoid instances of the compiler attempting to find a parent directory
            #Simply because there is a genre with a '/' in it.
            #Either way, this opens the file.
            if '/' not in key:
                yule_f = open('{}_yules.txt'.format(key),'w',encoding='utf8')
                
            else:
                key = key.split('/')
                yule_f = open('{}_{}_yules.txt'.format(key[0],key[1]),'w',encoding='utf-8')
            
            #Write to the individual file for a specific corpus
            for i in range(0,100):
                yule_f.write(top100_left[i][1]  + '{} '.format(str(top100_left[i][0]).rjust(35-len(top100_left[i][1]))) +
                             top100_right[i][1] + ' \t{}\t'.format(str(top100_right[i][0]).ljust(mx_r-len(top100_right[i][1])))+
                             '\n') 
            #close that file
            yule_f.close()
        
        #just keep going
        except IndexError:
            continue
    
    
def filter_out_key(key,corpora_word_dict):
    """
    Filter out a key (i.e. an entire corpus) from corpora_word_dict.
    """
    
    keys = [key for key in corpora_word_dict.keys()] #get keys
    
    #get all words excluding those from the specified corpus
    all_words_list  = [corpora_word_dict[k] for k in keys if k != key]
    
    #combine into list of dictionaries of words
    all_words = [set(w for d in all_words_list for w in [w for w in d.keys()])]
    
    #initialize the actual set of all words
    all_words_ = set()                     
    
    #populate set of all words, creating a catalogue of every
    #word in the the entire compendium 
    for set_ in all_words:
        all_words_ = all_words_.union(set_)
    
    #make it iterable
    all_words_ = list(all_words_)
    
    #create a word count dictionary of each word 
    master_word_dict = {w:0 for w in all_words_}
    
    #for each dictionary of words (each corpus excluding the specified one)
    for dicto in all_words_list:
        
        #get all of the words
        dict_words = [word for word in dicto.keys()]
        
        #add the word count for that word for this corpus to the master_word_dict
        
        for word in dict_words:
            master_word_dict[word]+=dicto[word]
            
    return master_word_dict

###############################################################################
#########################COMPUTE SYLLABIC TENDENCY#############################
###############################################################################

def avg_syls(lyrics):
    """
    Compute the average syllable count for a given set of lyrics. Parse through 
    each word, attempting to use nsyl (a more reliable/accurate function), and 
    if that fails, using syllables.

    """
    lyrics = lyrics.split() #convert to words
    
    syl_ct = 0 #initalize count
    for word in lyrics:
        try: #attempt and nsyl pull
            syl_ct += nsyl(word)[0]
        except:
            #otherwise count using in house algorithm
            print("Could not find syllables with nsyl for {}".format(word))
            syl_ct += syllables(word)
    
    #return the average syllable count
    return syl_ct/len(lyrics)
            


def nsyl(word):
    """
    Pull from the Carnegie Melon University's pronunciation dictionary to get
    syllable counts. Only works for 'real' words.
    
    Citation listed in file header under nltk resources.    
    """
    return [len(list(y for y in x if y[-1].isdigit())) for x in cmu[word.lower()]]

def syllables(word):
    """
    Calculate syllables of a word using a less accurate algorithm.
    Parse through the sentence, using common syllabic identifiers to count
    syllables.
    
    ADAPTED FROM: 
    [http://stackoverflow.com/questions/14541303/count-the-number-of-syllables-in-a-word]
    """
    #initialize count
    count = 0
    
    #vowel list
    vowels = 'aeiouy'
    
    #take out punctuation
    word = word.lower().strip(".:;?!")
    
    #various signifiers of syllabic up or down count
    if word[0] in vowels:
        count +=1
        
    for index in range(1,len(word)):
        if word[index] in vowels and word[index-1] not in vowels:
            count +=1
            
    if word.endswith('e'):
        count -= 1
        
    if word.endswith('le') or word.endswith('a'):
        count+=1
    
    if count == 0:
        count +=1
    
    if "ooo" in word or "mm" in word :
        count = 1
        
    if word == 'll':
        count = 0
    
    if (word.startswith('x') and len(word) >= 2) and word[1].isdigit():
        count = 0
    
    if word == 'lmfao':
        count = 5
    
    if len(word) < 2 and word not in ['a','i','y','o']:
        count = 0
        
    return count
    
def count_syls(strat_dict):
    
    """
    Count the syllables for each song in each corpus of an already
    stratified dictionary and return the average syllable count per corpus.    
    """
    
    #crack open keys    
    keys = [k for k in strat_dict.keys()]
    
    #make an empty dictionary
    avg_syls_per_corpus = {}
    
    #iterate over keys
    for key in keys:
        
        #isolate dictionary
        corpus = strat_dict[key]
        
        #empty list to be populated by individual syllable counts
        avg_syl_ct = []
        for row in corpus:
            #isolate and split lyrics
            lyrics = row['lyrics'].split()
            syl_ct = []
            for word in lyrics:
                
                try: #try to get the syllables using nsyl
                    syls = nsyl(word)
                    syl_ct.append(syls[0])
                    
                except KeyError: #otherwise use syllables
                    syls = [syllables(word)]
                    
                #append the answer to syl_ct 
                syl_ct.append(syls[0])   
            
            #average out the syllable counts for one song
            avg_syls = sum(syl for syl in syl_ct)/len(syl_ct)
            
            #add this to the corpus syllable count list
            avg_syl_ct.append(avg_syls)
        
        #average out the syllable counts for this corpus 
        avg_syll_corpus = sum(sylls for sylls in avg_syl_ct)/len(avg_syl_ct)
        
        #add this key, value pair to the dictionary of syllabic averages
        avg_syls_per_corpus[key] = avg_syll_corpus
     
    return avg_syls_per_corpus
    
###############################################################################
############################ PLOTTING CODE ####################################
###############################################################################
    
#nice colors
colors =[(0.9858982612571121, 0, 0),(0, 0.9303005088849723, 0),(0, 0, 0.7516498777431148)]


def plot_genre_by_yr_by_syllabic(choice):
    """
    Plot a time series of each genre's syllabic tendency from 1980 - 2010.
    Options for choice are 'all', 'together', 'indiv', and a list.
    
    'all'                ----> plot all genres, plus all individuals.
    'together'           ----> plot all specific genres together
    'indiv'              ----> plot one genre
    type(choice) == list ----> plot specific genres with eachother.
    
    """    
    
    #related to together
    fasttrack = False
    
    #clear figure
    plt.clf()
    
    #get the data by year
    year_dict = stratify_by_year('year')
    yr_keys = [key for key in year_dict.keys()] #crack open keys
    
    genre_dict = {'Rock':[],'Jazz':[],'Country':[],'Pop':[],'Dance':[],
                      'Hip-Hop':[],'R&B/Soul':[],'Latin':[],'Christian/Gospel':[]}
    
    #iterate over years
    for key in yr_keys:
        spec_year = year_dict[key]
        
        #add syllabic tendencies in year order to genre_dict ist
        for row in spec_year:
            avg_syl = avg_syls(row['lyrics'])
            genre   = row['genre']
            mini    = [int(key),avg_syl]
            genre_dict[genre].append(mini)
    
    #crack open genre_dict's keys
    genre_keys = [key for key in genre_dict.keys()]
    
    #all => indivs and together
    if choice == 'all':
        #set choice to together
        fasttrack = True
        choice = 'together'
    
    #plot all genres together
    if choice == 'together':
        
        genres = []
        for key in genre_keys:
            spec_genre = genre_dict[key]
            spec_genre.sort()
            X = [mini[0] for mini in spec_genre]
            Y = [mini[1] for mini in spec_genre]
            genre, = plt.plot(rand_jitter(X),Y,'o',color=(0,1-random.random(),random.random()),alpha=0.35,label=key)
            genres.append(genre)
        
        plt.title("Average Syllable Counts Per Word Per Song For Each Genre\nFrom 1980-2010")
        plt.ylabel('Average Syllables Per Song')
        plt.xlabel('Year')
        plt.legend(prop={'size':8},loc='best',frameon=False)
        plt.savefig('everyone.png')
    
    #clear the plot again
    plt.clf()
    if fasttrack:
        choice = 'indiv'
        
    #plot each indiviudal genre's syllabic average time series.       
    if choice == 'indiv':
        for key in genre_keys:
            spec_genre = genre_dict[key]
            spec_genre.sort()
            X = [mini[0] for mini in spec_genre]
            Y = [mini[1] for mini in spec_genre]
            plt.plot(rand_jitter(X),Y,'o',color=(random.random(),1-random.random(),0),alpha=0.4,label=key)
           
            plt.title("{}'s Syllabic Average from 1980-2010".format(key))
            plt.ylim(0,3)
            plt.xlabel("Year")
            plt.ylabel('Average Syllables Per Song')
            if len(key.split('/')) < 2:
                plt.savefig("{}.png".format(key))
        
            else:
                key = key.split('/')
                plt.savefig("{}_{}.png".format(key[0],key[1]))
            
            plt.clf()
            
            
    #plot specifics together     
    if type(choice) == list:
        title_str = ""
        title_str+=choice[0]+', '
        for i in range(1,len(choice)-1):
            title_str+="{}, ".format(choice[i])
        
        title_str+="and {}'s\nSyllabic Average from 1980-2010".format(choice[len(choice)-1])
        for choices in choice:
            spec_genre = genre_dict[choices]
            spec_genre.sort()
            X = [mini[0] for mini in spec_genre]
            Y = [mini[1] for mini in spec_genre]
            
            plt.plot(rand_jitter(X),Y,'o',color=colors[0],alpha=0.4,label=choices)
           
        plt.title(title_str)
        plt.ylim(0,3)
        plt.xlabel("Year")
        plt.ylabel('Average Syllables Per Song')
        plt.legend(loc='best',frameon=False)
        if len(key.split('/')) < 2:
            plt.savefig("group_photo.png".format(key))
        
        else:
            key = key.split('/')
            plt.savefig("group_photo".format(key[0],key[1]))
            
        plt.clf()
    
def rand_jitter(arr):
    """
    Jitter the points in the X in order to reduce overlap.    
    
    Taken directly from: 
    [http://stackoverflow.com/questions/8671808/matplotlib-avoiding-overlapping-datapoints-in-a-scatter-dot-beeswarm-plot]
    """
    stdev = .01*(max(arr)-min(arr))
    return arr + np.random.randn(len(arr)) * stdev
    
###############################################################################
######################### DATABASE ACCESS FUNCTIONS ###########################
###############################################################################
    
def create_new_table_genre_stats():
    """
    Create a table called genre_stats in song_records
    """
    curs.execute("CREATE TABLE genre_stats (yule text, syllabic_average text, top100_yules text, genre text)")
    conn.commit()

def create_new_table_year_stats():
    """
    Create a table called year_stats in song_records
    """
    curs.execute("CREATE TABLE year_stats (yule text, syllabic_average text, top100_yules text, year text)")
    conn.commit()
    
def write_to_genre_stats():
    """
    Write important Yule, syllable data to the new tables
    """
    genre_dict = stratify_by_genre()
    genres = [key for key in genre_dict.keys()]
    
    genre_cts = count_word_occurrence(genre_dict)    
    genre_yules = yule_batch(genre_cts)
    
    avg_syls_per_genre = count_syls(genre_dict)
    for genre in genres:
        syl_ct = avg_syls_per_genre[genre]
        top100 = [mini[0] for mini in genre_yules[genre][:100]]
        top100_words = [mini[1] for mini in genre_yules[genre][:100]]
        genre_yule_avg = sum(top100)/100
        top100_w = ','.join(top100_words)
        curs.execute("INSERT INTO genre_stats (yule,syllabic_average,top100_yules,genre) VALUES (?,?,?,?)",(genre_yule_avg,syl_ct,top100_w,genre))
        conn.commit()


def write_to_year_stats():
    """
    Write important Yule, syllable data to the new tables.
    """
    year_dict = stratify_by_year('decade')
    years = [key for key in year_dict.keys()]
    
    year_cts = count_word_occurrence(year_dict)    
    year_yules = yule_batch(year_cts)
    
    avg_syls_per_year = count_syls(year_dict)
    for year in years:
        syl_ct = avg_syls_per_year[year]
        top100 = [mini[0] for mini in year_yules[year][:100]]
        top100_words = [mini[1] for mini in year_yules[year][:100]]
        year_yule_avg = sum(top100)/100
        top100_w = ','.join(top100_words)
        curs.execute("INSERT INTO year_stats (yule,syllabic_average,top100_yules,year) VALUES (?,?,?,?)",(year_yule_avg,syl_ct,top100_w,str(year)))
        conn.commit()
###############################################################################
        
def main():
    genre_dict = stratify_by_genre() #get song data stratified by genre
    
    corpora_word_dict = count_word_occurrence(genre_dict)  #get word counts for each genre
    yules = yule_batch(corpora_word_dict) #get the yule coefficient for
                                          #each genre against the rest
                                          #of the dataset
    
    write_yules_to_file(yules) #write to a file
main()

conn.close()
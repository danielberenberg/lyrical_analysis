# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 20:59:05 2016

@author: danberenberg
"""

"""
   _________________________________________________________________
  |                                                                 |
  | CS/STAT 287 FINAL PROJECT: DATA EXTRACTION FILE                 |
  |                                                                 |
  | This file is intended to populate a database of music metadata  |
  | by accessing several lyrics/music website APIs. The end result  |
  | is a locally located database file called 'song_records.db'     |
  | with 2421 rows, each containing the song name, artist name,     |
  | lyrics, genre, and production year in each column for the top   |
  | 100 songs of each year between 1980 and 2010.                   |
  |                                                                 |
  | The file uses two web APIs (musixmatch, chartlyrics) and one    |
  | website with a simple enough URL 'equation' to access pages     |
  | programmatically (jamrockentertainment).                        | 
  |                                                                 |
  | The routine runs as follows:                                    |
  |                                                                 |
  |    (1) Set up file by:                                          |
  |                                                                 |
  |         (a) making several imports                              |
  |         (b) connect to database                                 |
  |                                                                 |
  |    (2) find_names(year)                                         |
  |                                                                 |
  |         Access the website [jamrockentertainment.com]. This     |
  |         website contains the top 100 song for each year website |
  |         between 1980 and 2010. Create a dictionary containing   |
  |         all future rows for a given year.                       |
  |                                                                 |
  |    (3) find_lyrics(song,artist,dict_year)                       |
  |                                                                 |                      
  |         Access the chartlyrics API and grab the lyrics for each |
  |         song and artist, add the lyrics to the dictionary for   |
  |         that year.                                              |
  |                                                                 |
  |    (4) find_genre(song,artist,dict_year)                        |
  |                                                                 |
  |         Access the musixmatch API and grab the genre for        |
  |         each song and artist, add the lyrics to the dictionary  |
  |         for that year.                                          |
  |                                                                 |
  |    (5) Write the all the data for a given year to the database. |
  |_________________________________________________________________|
  
  ADAPTED FROM:

  Beautiful Soup citations
 
   -->[http://stackoverflow.com/questions/18453176/removing-all-html-tags-along-with-their-content-from-text]
   -->[http://stackoverflow.com/questions/1765848/remove-a-tag-using-beautifulsoup-but-keep-its-contents]
   -->[https://www.crummy.com/software/BeautifulSoup/bs4/doc/#find]
 
 
  urllib citations

   -->[https://docs.python.org/2/library/urllib.html]
 
  sqlite3 citations
   -->[https://docs.python.org/2/library/sqlite3.html]
   -->[http://www.sqlitetutorial.net/sqlite-delete/]
   -->[http://www.w3schools.com/sql/sql_select.asp]
   -->[https://www.sqlite.org/lang_update.html]
   -->[https://www.tutorialspoint.com/sql/sql-update-query.htm]
   -->[http://stackoverflow.com/questions/305378/list-of-tables-db-schema-dump-etc-using-the-python-sqlite3-api]

  requests citations
   -->[http://docs.python-requests.org/en/master/]
   
  ADDITIONAL RESOURCES:
   -->[http://www.chartlyrics.com/api.aspx]
   -->[https://developer.musixmatch.com/documentation]
   -->[http://www.programmableweb.com/api/chartlyrics-lyric]
   
   
"""

# Set up file (1) #
from bs4 import BeautifulSoup
from urllib import request
import requests
import sqlite3
from MUSIC_MATCH_ID import API_KEY

#musixmatch api
api = API_KEY.lstrip()


try: #connect to database
    connect = sqlite3.connect("song_records.db")
    curs    = connect.cursor()
    print("Connection Successful")

except:
    print("Could not connect to sqlite3 db...")

    
###############################################################################
############################# FIND FUNCTIONS ##################################
###############################################################################
    
def find_names(year): # (2)
    """
    Get song names/artists for a given year from jamrockentertainment. Group the
    rank of the song for that year, the song name, and the artist into a list of
    length 3, append that list to a larger list. 
    
    Convert the larger list of smaller lists to a dictionary in which keys are
    lists of two, song and artist, and values are lists of the genre, the lyrics, 
    the rank, and the year.
    
    Input:
        -year for which names shall be extracted
    
    Output:
        -dictionary that will be converted later into rows of the database
    """
    
    url_addr = "http://www.jamrockentertainment.com/billboard-music-top-100-songs-listed-by-year/top-100-songs-{}.html".format(year)
    html_doc = get_html_doc(url_addr)
    
    #get parser
    soup = BeautifulSoup(html_doc,'html.parser')
    
    #parse through contents, find top 100 songs, artists of that year
    relevant_info = [tag.get_text() for tag in soup.find_all('td')]
    hold_list = []
    
    #build a dictionary that maps the string SONG>ARTIST
    #to the genre, lyrics, year, and rank 
    
    grouped_by_3 = []  #precursor list and routine to group relevant info together
    for i in range(0,len(relevant_info),3):
        hold_list = [relevant_info[i],relevant_info[i+1], relevant_info[i+2]]
        grouped_by_3.append(hold_list)
    
    #assemble dictionary
    grouped_dict = {"{}>{}".format(mini[1],mini[2]): {'genre': None,'lyrics': None,'year' : year,'rank' : mini[0], 'mm_ID':None} for mini in grouped_by_3}
    
    return grouped_dict

def find_lyrics(song,artist,grouped_dict): # (3)
    """
    Find the lyrics of a given song (through musixmatch API) and update the 
    dictionary containing that song with the sanitized lyrics.    
    
    Inputs:
        -song name
        -artist name
        -dictionary in which that song,artist meta data is contained
    
    Output:
        -1 if successful lyrics scrape, 0 if unsuccessful
    """    
    
    #modify song,artist to fit api
    ref_song, ref_artist = form_to_mm_api(song),form_to_mm_api(artist)
     
    #set up request
    cmnd = "http://api.chartlyrics.com/apiv1.asmx/SearchLyricDirect?artist={}&song={}".format(ref_artist,ref_song)
    r    = requests.get(cmnd)
     
    
    if r.status_code == 200: # => successful connection
    
        print('Successful connection for {} by {}'.format(song,artist))
        
        #make a beautiful soup
        html_doc = r.content
        soup     = BeautifulSoup(html_doc,'html.parser')
        
        #extract and sanitize lyrics
        lyrics   = soup.find_all('lyric')
        lyrics   = remove_chars(str(lyrics),'lyrics')
         
        #add appropriately to dictionary
        grouped_dict['{}>{}'.format(song,artist)]['lyrics'] = lyrics
        
        return 1 # ==> successful scrape
         
    else:
        print("Unsuccessful connection for {} by {}".format(song,artist))
        
        #delete that entry (impute the row)
        
        del grouped_dict['{}>{}'.format(song,artist)]
        return 1 # ==> unsuccessful scrape
         
def find_genre(song,artist,grouped_dict): # (4)
    """
    Find the genre of a given song by accessing the musixmatch API. 
    Populatetes into the grouped_dict the genre of each song, or
    completely removes an entry that does not have an accessable page on musixmatch.
    
    Inputs:
        -song name
        -artist name
        -dictionary in which that song,artist meta data is containedy
        
    Outputs:
        -1 if successful lyrics scrape, 0 if unsuccessful
        
    """
    #edit song,artist strings to get ready for the api request
    ref_song, ref_artist = form_to_mm_api(song),form_to_mm_api(artist)
    base_cmd = "http://api.musixmatch.com/ws/1.1/"
    
    #request
    r = requests.get('{}track.search?apikey={}&q_track={}&q_artist={}&f_has_lyrics=1'.format(
        base_cmd,api,ref_song,ref_artist))

    
    if (r.status_code == 200): #==>successful connection
        
        try: # try this routine because sometimes the returned json is corrupted
            
            #get primary genre list
            pr_genre_list = r.json()['message']['body']['track_list'][0]['track']['primary_genres']['music_genre_list']
            
            #filter to genre names
            genres = [pr_genre_list[i]['music_genre']['music_genre_name'] for i in range(0,len(pr_genre_list))]
            
            #join as string
            genres = ",".join(genres)
            
            #write whole string to dictionary
            grouped_dict['{}>{}'.format(song,artist)]['genre'] = genres
            
            print("Successful connection for {} by {}".format(song,artist))
            
            return 1 # ==> successful genre scrape
        
        except: #something went wrong with json
            print("Unsuccessful connection for {} by {}".format(song,artist))
            return 0 #==> unsuccessful genre scrape
        
    else: # ==> unsuccessful connection
        print("Unsuccessful connection for {} by {}".format(song,artist))
        return 0 # ==> unsuccessful genre scrape

###############################################################################
########################## DB ACCESS FUNCTIONS ################################
###############################################################################
def write_dict_to_DB(dict_year): # (5)
    
    """
    Write most of the song meta data to the database, excluding genre. 
    Parse through dict_year, writing all the values of each key and the key
    itself to the database.
    
    Input:
        dict_year: the dictionary of song meta data for a given year.
        
    Output:
        None
    
    """
    keys = [key for key in dict_year.keys()] #organize names into indexed structure
    for key in keys:
        key_spl = key.split(">") #crack key open
        
        #write to sqlite3 database
        curs.execute("INSERT INTO media (artist_name,song_name,lyrics,year) VALUES (?,?,?,?)",
        (key_spl[1],key_spl[0],dict_year[key]['lyrics'],str(dict_year[key]['year'])))
        
        #make it real
        connect.commit()

def write_genre_to_DB(dict_year): # (5)
    """
    Update each row of the database to include the genre(s) of the song. Parse 
    through dict_year, writing genre of each key and the key itself to 
    the database.
    
    """
    
    keys = [key for key in dict_year.keys()] #indexed structure
    for key in keys:
        k_spl = key.split(">") #crack key open
        
        if dict_year[key]['genre'] != None: #make sure genre was successfully grabbed
            curs.execute("UPDATE media SET genre = ? WHERE song_name = ? AND artist_name = ?",
                         (dict_year[key]['genre'],k_spl[0],k_spl[1]))
        
        else: #otherwise impute entire row
            curs.execute("DELETE FROM media WHERE song_name = ? AND artist_name = ?",
                         (k_spl[0],k_spl[1]))
                         
        connect.commit() #make it real
        
###############################################################################
############################ HELPER FUNCTIONS #################################
###############################################################################
def form_to_mm_api(string): #helper method
    """
    Modify a string (namely an artist or song) to the musixmatch API.    
    
    Inputs:
        -string
    
    Output:
        -musixmatch ready string
        
    Ex:
        form_to_mm_api('Call Me') = Call%20Me
    """
    s_split = string.lower().split() #break it up
    if len(s_split) > 1:
        return "%20".join(s_split) #put it together
        
    return string #send it away

def remove_chars(string,choice): #helper method
    """
    Remove irrelevant characters from a string specified by exclude set for
    either a lyrics string or title string. 
    
    Inputs:
        -string: Either a titlestring (song,artist) or lyrics string
        -choice: either 'lyrics' or 'title'
    
    Output:
        -lowercase, charless string
    Ex:
    remove_chars("Hello, World",['e','title') returns "Hllo World"
    """
    exclude = ['!', '"', '#', '$', '%', '&',   #exclude set
               "'", '(', ')', '*', '+', ',', 
               '-', '.', '/', ':', ';', '<',
               '=', '>', '?', '@', '[', '\\', 
               ']', '^', '_', '`', '{', '|', 
               '}', '~',"'", '\n', '\r',
               '<lyric>','</lyric>']
               
    for ch in exclude: #iterate over exclude set, remove remove and
                       # replace accordingly
        if ch in string and choice == 'lyrics':
            string = string.replace(ch," ")
            
        if ch in string and choice == 'title':
            string = string.replace(ch,"")
            
    return string.lower() #return charless string
        
def get_html_doc(url_addr): #helper method
    """
    Prepare an html document to be parsed into soup.
    
    Input:
        -url_addr : the string containing a url address
    
    Output:
        -The html document of the site accessed.
    
    """
    url = request.urlopen(url_addr) #open url
    html_doc = url.read() #grab doc
    return html_doc #return it

def get_top_lyrics(year): #batch method
    """
    Get the top lyrics for a given year. This is a function that organizes large
    batches of instances of the find_lyrics(song,artist,dict_year function.
    
    Note: the functions get_top_lyrics and get_top_genres are very similar and could
    be combined to get all data in one foul swoop. I build the database piece by
    piece instead so there are two nearly identical functions.

    Input:
        -year in question
        
    Output:
        -A dictionary of song metadata including the lyrics and year
        of production
    
    """
    dict_year = find_names(year) #fetch all the names
    keys = [key for key in dict_year.keys()] #organize names into indexed structure
    successful_scrapes = 0 #initialize counter var
    for key in keys: 
        key = key.split(">") #crack the keys open
        successful_scrapes += find_lyrics(key[0],key[1],dict_year) # find the lyrics, add 1 or 0 to
                                                                   # the counter 
        
    #print the amount of lyrics successfully scraped
    print("Scraped lyrics of {}/100 songs for the year {}".format(successful_scrapes,year))
    
    return dict_year #send dict_year away to be written to the database

def get_top_genres(year): #batch method
     """
    Get the genres for top 100 of a given year. This is a function that organizes large
    batches of instances of the find_genre(song,artist,dict_year) function.
    
    Note: the functions get_top_lyrics and get_top_genres are very similar and could
    be combined to get all data in one foul swoop. I build the database piece by
    piece instead so there are two nearly identical functions.

    Input:
        -year in question
        
    Output:
        -A dictionary of song metadata including the genre and year
        of production
    
    """
     dict_year = find_names(year)  #get all the names for the year
     successful_scrapes = 0 #initialize counter var
     keys = [key for key in dict_year.keys()] #organize names into indexed structure
     for key in keys:
         key = key.split(">") #crack key open
         
         #increment scrape counter by 0 or 1
         successful_scrapes += find_genre(key[0],key[1],dict_year) 
      
     #print the amount of genres successfully scraped
     print("Scraped genres of {}/100 songs for the year {}".format(successful_scrapes,year))
     
     return dict_year #send away to be written to database
def filter_genres():
    
    """---------------------------------------------------------------------------- * 
 |  Filter the genres of each song to single genre.                                 | 
 |                                                                                  |
 |  A weighting algorithm is performed so that each each genre is preferenced       |
 |  based on subgenres.                                                             |
 |                                                                                  |
 |  Each subgenre is assigned a number between 0 and 1 indicating how               |
 |  much that subgenre 'proves' a song is the subgenre's parent.                    |
 |                                                                                  |
 |  The weighting takes place with the following intentions:                        |
 |      a) Try to be as 'correct' as possible.                                      |
 |                                                                                  |
 |      b) Avoid choosing 'Pop' as a genre at all costs. Pop is a genre that        |
 |        aggregates other genres. Although all of these genres are rather broad    |
 |        (or very specific), Pop stands out as a genre that does not intrinsically |
 |        filter music type.                                                        |
 |                                                                                  |
 |  Example:                                                                        |
 |                                                                                  |
 |     A song with genres Rock, Alternative, Pop, Heavy Metal, and Rap              |
 |     will be weighted so that There are 3 Rock "votes", 1 Pop "vote"              |
 |     and 1 Rap "vote". In this case, the song is classified as Rock.              |
 |                                                                                  |
 * -------------------------------------------------------------------------------- *
 |                     GENRE AND SUBGENRE CLASSIFICATION                            |
 * -------------------------------------------------------------------------------- *
 |                                                                                  |
 |       Rock                  Jazz         Country          Pop                    |
 |     ---------              --------      ---------       -----                   |
 |     -Alternative           -Jazz         -Country        -Soundtrack             |
 |     -Heavy Metal                                         -Vocal                  |
 |     -Singer/Songwriter                                   -Easy Listening         |
 |     -New Wave                                            -Holiday                |
 |     -Pop/Rock                                            -Pop                    |
 |     -American Trad Rock                                                          |
 |     -Rock                                                                        |
 |     -Indie Rock                                                                  |
 |                                                                                  |
 |      Dance               Hip-Hop          R&B/Soul       Latin                   |
 |     -------             ---------        ----------     -------                  |
 |     -Dance               -Hip Hop/Rap     -Disco         -Pop in Spanish         |
 |     -Electronic          -Hardcore Rap    -Soul          -Latin                  |
 |                          -Hip-Hop         -R&B/Soul      -Reggae                 |
 |                          -Rap                            -Brazilian              |
 |                                                          -Latin Urban            |
 |      Christian/Gospel                                    -Salsa y Tropical       |
 |     ------------------                                                           |
 |      -Christian/Gospel                                                           |
 |      -Holiday                                                                    |                                                             
 |                                                                                  |
 * -------------------------------------------------------------------------------- *                                                                                  
 |      WEIGHTING                                                                   |
 * -------------------------------------------------------------------------------- *
 |                                                                                  |
 |      Subgenre               Parent Genre     Weight                              |
 |      --------               ------------     ------                              |
 |      Alternative              Rock             0.6                               |
 |      Heavy Metal              Rock             0.8                               |
 |      Singer/Songwriter        Rock             0.6                               |
 |      Pop/Rock                 Rock             0.9                               |
 |      American Trad Rock       Rock             0.9                               |
 |      Tribute                  Rock             0.4                               |
 |      Rock                     Rock             0.9                               | 
 |      Jazz                     Jazz            10.0 (Special case)                |
 |      Country                  Country          3.0 (Special case)                |
 |      Soundtrack               Pop              0.7                               |
 |      Pop                      Pop              0.5                               |
 |      Easy Listening           Pop              0.5                               |
 |      Vocal                    Pop              0.6                               |
 |      Dance                    Dance            1.0                               |
 |      Electronic               Dance            0.8                               |
 |      New Wave                 New Wave         1.0                               |
 |      Hip-Hop                  Hip-Hop          1.0                               |
 |      Hip Hop/Rap              Hip-Hop          1.0                               |
 |      Hardcore Rap             Hip-Hop          1.0                               |
 |      Rap                      Hip-Hop          1.0                               |
 |      R&B/Soul                 R&B/Soul         0.8                               |
 |      Soul                     R&B/Soul         0.8                               |
 |      Disco                    R&B/Soul         0.9                               |
 |      Latin                    Latin            1.0                               |
 |      Pop in Spanish           Latin            1.0                               |
 |      Reggae                   Latin            1.0                               |
 |      Latin Urban              Latin            1.0                               |
 |      Salsa y Tropical         Latin            1.0                               |
 |      Christian/Gospel         Christian/Gospel 5.0                               |
 |      Holiday                  Christian/Gospel 5.0                               |
 * -------------------------------------------------------------------------------- *  
   """
    
    #Make weights
    genre_weights = {'Rock'   : {'Alternative':0.6, 'Heavy Metal':0.8,
                                 'Singer/Songwriter':0.6, 'Pop/Rock':0.9,
                                 'American Trad Rock':0.8,'Rock':1.0,'New Wave':1.0},          
                    'Hip-Hop' : {'Hip-Hop':1.0,'Hip Hop/Rap':1.0,
                                 'Hardcore Rap':1.0, 'Rap':1.0},
                    'R&B/Soul': {'R&B/Soul':0.9,'Soul':0.9,'Disco':0.9},
                    'Latin'   : {'Latin':1.0,'Pop in Spanish': 1.0, 'Reggae': 1.0,
                                 'Latin Urban':1.0,'Salsa y Tropical':1.0},
                    'Dance'   : {'Dance':1.0,'Electronic':1.0},
                    'Country' : {'Country': 3.0},
                    'Jazz'    : {'Jazz': 10.0},
                    'Pop'     : {'Soundtrack':0.7,'Pop':0.5, 'Vocal':0.6,
                                 'Easy Listening': 0.5,'Holiday':1.0},
                    'Christian/Gospel': {'Christian/Gospel':10.0}}
    
    #Make it iterable
    prim_genre_weight_keys = [key for key in genre_weights.keys()]
    
    curs.execute("SELECT genre, song_name, artist_name FROM media")
    data = curs.fetchall()
    
    
    all_genres = [entry[0] for entry in data]
    all_songs = [entry[1] for entry in data]
    all_artists = [entry[2] for entry in data]
    
    for i in range(len(all_genres)):
        indiv_weights = {'Rock':0,'Hip-Hop':0,'R&B/Soul':0,'Latin':0,
                         'Dance':0,'Country':0,'Jazz':0,'Pop':0,'Christian/Gospel':0}
        artist = all_artists[i] #stored as tuple
        song   = all_songs[i]   #stored as tuple
        genres = all_genres[i] #genres tored as comma separated list
        genres = genres.split(",") #split genres
        for g in genres:
            for pg in prim_genre_weight_keys: #pg = primary genre 
                
                subgenres = [sg for sg in genre_weights[pg].keys()] #sg = subgenre
                
                if g in subgenres:
                    for sg in subgenres:
                        if g == sg:
                            indiv_weights[pg]+=genre_weights[pg][sg]
        
        indiv_weight_keys = [k for k in indiv_weights.keys()]
        top_genre_wt = 0
        top_genre    = ""
        
        for k in indiv_weight_keys:
            if indiv_weights[k] > top_genre_wt:
                top_genre = k
        
        print("Updating {} - {} to {}".format(song,artist,top_genre))
        curs.execute("UPDATE media SET genre = ? WHERE artist_name = ? AND song_name = ?",(top_genre,artist,song))
        
        connect.commit()
        
def hand_enter():
    """
    An unfortunate, painstaking function used to fill in missing genres
    for partially complete data points.   
    
    There were 491 data points without a genre. This function has been left here
    in remembrance.
    """
    
    #get all rows in question
    curs.execute("SELECT song_name, artist_name FROM media WHERE genre = ?",("Holiday",))
    
    genre_less = curs.fetchall()
    
    #slowly and painstakingly classify their genres...
    for i in range(len(genre_less)):
        row = genre_less[i]
        print(row)
        print("{} songs left!".format(len(genre_less)-i))
        update_g = input("What is this song's genre? ")
        curs.execute("UPDATE media SET genre = ? WHERE artist_name = ? AND song_name = ?",(update_g,row[1],row[0]))
        connect.commit()
        
        quit_ = input("Want to quit? ")
        
        if quit_ == 'y':
            break
        
def main():
    """
    Main method where code is executed.    
    
    """
    curs.execute("SELECT * FROM media WHERE song_name =? ",("Call Me",))
    genres = curs.fetchall()  
    print(genres)
    pass
main() #execute main

connect.close() #close database conection
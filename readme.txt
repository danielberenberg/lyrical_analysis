
Dan Berenberg
CS/STAT 287 Final Project
readme
————————————————————————————————————————————————————————————————————————————

I.Introduction

Hello! Welcome to Daniel Berenberg’s Final Project suite! Here you will find a short explanation of each .py file located in this directory. 

————————————————————————————————————————————————————————————————————————————

II. Overview

This project was meant to create a large enough data set to analyze and make inferences on the underlying trends and tendencies of musical lyrics. 

Each of the .py files is called data_X.py where X = xtraction, analysis, or inference. The bulk of the work is done in data_xtraction.py and data_analysis.py, the third file is more of a throwaway attempt that is included to show that there is definitely work to be done with this data set.

There is also a build_db.py file that builds the database (that’s it) and a file
called MUSIC_MATCH_ID.py that hold the API key for musixmatch API. The key was included in the directory just in case.

Please see an overview of each file below.

————————————————————————————————————————————————————————————————————————————

III. data_xtraction.py

This file populates the database song_records.db (specifically the media table) and performs some cleaning operations.

Inside of this file, you will find functions that grab song metadata off of the web from various APIs (musixmatch, chart lyrics, and jamrockentertainment) and writes the data found in this way to a database. 

You will also find a cleaning function that uses a weighting system to classify genres of each song down to one genre. The docstring for that function is incredibly detailed, so the time is not wasted here to repeat and explanation.

————————————————————————————————————————————————————————————————————————————

IV. data_analysis.py

This file performs a Yule coefficient analysis on the data of each genre/production year and reports the most common words in one genre/year versus the rest of the dataset. 

The file also runs an analysis of syllabic averages and plots the time series of syllabic average progression per genre.

From these analyses, various inferences are made about the data set and music in general. 

————————————————————————————————————————————————————————————————————————————

V. data_inference.py

This file was intended to be a classifier model that uses a weighting scheme related to the syllabic average and Yule coefficients of a given lyrics set compared to the data set already compiled. Any words in the lyrics that are not in the data set would not be counted.

Unfortunately, this file was not completed, but has been attached anyway.


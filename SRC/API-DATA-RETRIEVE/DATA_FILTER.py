###IN THIS PY CODE WE FILTER THE DATA THAT WE RETREIVED###
###FROM THE API AND REFORMS IT INTO THE CORRECT TABLES###

import csv
import sys
import requests
import time
import ast

###this function is used to enable reading huge text (by csv package)###
###that is stored in some csv cells###
def massive_text():
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

###the api returns extra details that we don't need###
###this function filter the data that we don't need###
###from csv files###
def reformer(filename, new_headers, old_headers):
    new_data = []
    with open("../" + filename, 'r', encoding="utf-8") as f:
        for line in csv.DictReader(f):
            d = {}
            for old_header, new_header in zip(old_headers, new_headers):
                d[new_header] = line[old_header]
            new_data.append(d)
    with open("dbnew_" + filename, 'w', encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=new_headers)
        writer.writeheader()
        writer.writerows(new_data)

###collects data from a csv file and reforms in another csv file###
def data_collector1(st):
    with open('../Credits.csv', 'r', encoding="utf-8") as f:
        s = 'role' if st=='cast' else 'job'
        for line in csv.DictReader(f):
            if (line['id'] != 'NULL'):
                temp = line['id']
                for dic in ast.literal_eval(line[st]):
                    val = {}
                    val['movie_id'] = temp
                    if ('id' in dic):
                        val[st + '_id'] = dic['id']
                    else:
                        val[st + '_id'] = None
                    if ('job' in dic):
                        val[s] = dic['job']
                    else:
                        val[s] = None
                    data.append(val)
    return data

###collects data from a csv file and reforms in another csv file###
def data_collector2(st):
    data = []
    with open('Movies.csv', 'r', encoding="utf-8") as f:
        for line in csv.DictReader(f):
            if (line['movie_id'] != 'NULL'):
                temp = line['movie_id']
                for dic in ast.literal_eval(line['genres']):
                    val = {}
                    val['movie_id'] = temp
                    val['genre_id'] = dic['id']
                    data.append(val)
    return data

###uses the above function with several values###
###used to create Movie_Actors, Movie_Genre, Movie_Crew, Movie_Company###
def proc(st):
    s = 'role' if st=='cast' else 'job'
    headers = ['movie_id', st + '_id', s]
    if("actors" in st or "crew" in st):
        data = data_collector1(st)
    else:
        data = data_collector2(st)
        headers = headers[:-1]
    with open('../Movie_' + st[0].upper() + st[1:] + '.csv', 'w', encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    massive_text()
            

###IN THIS PY CODE WE RETRIEVE ALL THE NEEDED DATA###
###FROM THE API AND SAVE IT IN CSV FILES###

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

###returns the movies ids that is stored in a csv file###
def get_movies_ids(filename):
    with open(filename, 'r') as f:
        r = csv.reader(f, delimiter = ';')
        ids = [i[0] for i in r]
    return ids

###returns the people ids (crew/cast) that is stored in a csv file###
def get_people_ids(field):
    ids = []
    with open('Credits.csv', 'r', encoding="utf-8") as f:
        for line in csv.DictReader(f):
            if(line['id'] != 'NULL'): 
                ids += [p['id'] for p in ast.literal_eval(line[field])]
    return list(set(ids))

###returns the ids of the companies that produced the movies in our db###
def get_companies_ids(filename):
    ids = set()
    flag = True
    with open(filename, 'r',  encoding="utf-8") as f:
        for line in csv.DictReader(f):
            if flag:
                flag = False
                continue
            for j in ast.literal_eval(line['production_companies']):
                try:
                    ids.add(j['id'])
                except:
                    print(j)
    return ids

###returns the types of data that the api (at link) gives us###
def get_table_headers(i, link):
    rq = requests.get(link.format(i, key))
    headers = list(rq.json().keys())
    return headers

###retrieve data from the api (at "link") by passing "ids" to it and stores###
###it to a file "tofile" with the certain "headers" that we retrieve###
###from other function.###
###we use "credits" boolian value to store important data about people###
###that will help us later to ask the api about it###
def get_data_from_api(ids, link, headers, tofile, credit):
    data = []
    global cast_ids
    global crew_ids
    with open(tofile, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
    for c, i in enumerate(ids):
        while(True):
            try:
                rq = requests.get(link.format(i, key))
                json = rq.json()
                break
            except:
                time.sleep(5*60)
        dic = {}
        for header in headers:
            dic[header] = "NULL"
            if(header in json):
                dic[header] = json[header]
        data.append(dic)
        if(c % 100 == 0):
            while(True):
                try:
                    with open(tofile, 'a', newline='', encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writerows(data)
                        data = []
                    break
                except Exception as e:
                    print(e)
                    time.sleep(15)
        if(credit):
            try:
                cast_ids += [cast['id'] for cast in dic['cast'] if(dic['cast'] != 'NULL' and cast['id'] not in cast_ids)]
                crew_ids += [crew['id'] for crew in dic['crew'] if(dic['crew'] != 'NULL' and crew['id'] not in crew_ids)]
            except Exception as e:
                print(e)

                
if __name__ == "__main__":
    massive_text()
    ###api key###
    key = "8c50a74ae91317696e5ea566240ae405"
    ###apis links###
    details_link = "https://api.themoviedb.org/3/movie/{0}?api_key={1}&language=en-US"
    credits_link = "https://api.themoviedb.org/3/movie/{0}/credits?api_key={1}&language=en-US"
    people_link = "https://api.themoviedb.org/3/person/{0}?api_key={1}&language=en-US"
    companies_link = "https://api.themoviedb.org/3/company/{0}?api_key={1}"
    ###needed ids###
    cast_ids = []
    crew_ids = []
    movies_ids = get_movies_ids('movies_ids.csv')
    movie = movies_ids[0]
    cast_ids = get_people_ids('cast')
    crew_ids = get_people_ids('crew')
    companies_ids = get_companies_ids("Movies.csv")
    ###retreive data from api###
    get_data_from_api(movies_ids, details_link, get_table_headers(movie, details_link), 'Details.csv', False)
    get_data_from_api(movies_ids, credits_link, get_table_headers(movie, credits_link), 'Credits.csv', True)
    get_data_from_api(cast_ids, people_link, get_table_headers(cast_ids[0], people_link), 'Cast.csv', False)
    get_data_from_api(crew_ids, people_link, get_table_headers(crew_ids[0], people_link), 'Crew.csv', False)
    get_data_from_api(companies_ids, companies_link, get_table_headers(companies_ids[0], companies_link), "Companies.csv")

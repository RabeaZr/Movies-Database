###IN THIS CODE WE INSERT THE DATA RETREIVED FROM THE API###
###TO OUR DATABASE###

import mysql.connector as connector
import csv

cursor, conn = None, None

###returns the columns names of a csv file###
def get_headers(tname):
   with open("../{0}.csv".format(tname), 'r', encoding="utf-8") as f:
        headers = list([line for line in csv.DictReader(f)][0].keys())
   return headers

###returns the currect columns types for each table in the database###
def get_fields_types(tname):
   headers = get_headers(tname)
   if(tname == "Movies"):
      return ["VARCHAR"]*2 + ["INT"]*3 + ["BIGINT", "DATE", "INT"] + ["DOUBLE"]*2 + ["VARCHAR"]*2 + ["TEXT"]

   elif(tname == "Movie_Genre" or tname == "Movie_Company"):
      return ["INT"]*2

   elif(tname == "Movie_Crew" or tname == "Movie_Actors"):
      return ["INT"]*2 + ["VARCHAR"]

   elif(tname == "Genres"):
      return ["INT", "VARCHAR"]

   elif(tname == "Companies"):
      return ["INT"] + ["VARCHAR"]*2

   elif(tname == "Crew" or tname == "Actors"):
      return ["VARCHAR", "INT", "VARCHAR"] + ["DATE"]*2 + ["INT"] + ["VARCHAR"]*2 + ["DOUBLE"]

###pads zeros to a date day and month if needed###
def pad(d):
   if(len(d) == 1):
      return '0' + d
   return d

###gets the values from the csv file and returns it well formatted###
###relatively to the database###
def reformat_values(values):
   vals = []
   for v, t in values:
      if(v == '' or v == "NULL"):
         val = "NULL" if t != "DATE" else "1000-01-01"
         vals.append(val)
         continue
      if(t == "INT" or t == "BIGINT"):
         vals.append(int(v))
      elif(t == "DOUBLE"):
         vals.append(float(v))
      elif(t == "DATE"):
         #might be a different format
         d = v.split('-')
         vals.append("{YYYY}-{MM}-{DD}".format(YYYY = d[0], MM = pad(d[1]), DD = pad(d[2])))
      else:
         vals.append(v)
   return tuple(vals)

###inserts values from csv files to the database###
def insert_to_table(tname, i = 0):
   types = get_fields_types(tname)
   errs = []
   with open('../{0}.csv'.format(tname), 'r', encoding="UTF-8") as f:
      for line in list(csv.DictReader(f))[i:]:
         try:
            cursor.execute("INSERT INTO {0} VALUES {1};".format(tname, reformat_values(zip(list(line.values()), types))))
            if(i % 1000 == 0):
               print(i)
            i += 1
         except Exception as e:
            errs.append((line, e))
   conn.commit()
   return errs

###connects to database and defines the cursor###
def connect_to_db():
   global cursor
   global conn
   conn = connector.connect(
      host='localhost',
      port=3305,
      user='DbMysql53',
      password='DbMysql53',
      database='DbMysql53'
   )
   cursor = conn.cursor()

###performs the insertions to the database on all tables###
###and closes connection at the end with the database###
if __name__ == "__main__":
   connect_to_db()
   tables = ["Movies", "Genres", "Companies", "Crew", "Actors", "Movie_Genre", "Movie_Company", "Movie_Crew", "Movie_Actors"]
   for table in tables:
      errs = insert_to_table(table)
      print(errs)
   cursor.close()
   conn.close()

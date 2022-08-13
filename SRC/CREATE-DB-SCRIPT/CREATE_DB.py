import mysql.connector as connector
import csv

cursor, conn = None, None

###returns the columns names of a csv file###
def get_headers(tname):
   with open("../{0}.csv".format(tname), 'r', encoding="utf-8") as f:
        headers = list([line for line in csv.DictReader(f)][0].keys())
   return headers

###returns the max field size of field in table named tname###
def get_max_field_size(tname, field):
   with open("../{0}.csv".format(tname), 'r', encoding="utf-8") as f:
      res = max([len(line[field]) for line in csv.DictReader(f)])
   return res

###returns the currect columns types for each table in the database###
###and the foreign keys and primary keys for each table###
def get_fields_types(tname):
   headers = get_headers(tname)
   if(tname == "Movies"):
      m1 = get_max_field_size(tname, headers[0])
      m2 = get_max_field_size(tname, headers[1])
      m3 = get_max_field_size(tname, headers[-3])
      m4 = get_max_field_size(tname, headers[-2])
      m5 = get_max_field_size(tname, headers[-1])
      return [["VARCHAR({0})".format(m1), "VARCHAR({0})".format(m2)] + ["INT"]*3 + ["BIGINT", "DATE", "INT"] + ["DOUBLE"]*2 + ["VARCHAR({0})".format(m3), "VARCHAR({0})".format(m4), "TEXT({0})".format(m5)], "movie_id", []]

   elif(tname == "Movie_Genre" or tname == "Movie_Company"):
      return [["INT"]*2, "{0}, {1}".format(headers[0], headers[1]), [[headers[0], "Movies"], [headers[1], "Genres"*("Genre" in tname) + "Companies"*("Company" in tname)]]]

   elif(tname == "Movie_Crew" or tname == "Movie_Actors"):
      m1 = get_max_field_size(tname, headers[-1])
      return [["INT"]*2 + ["VARCHAR({0})".format(m1)], "{0}, {1}, {2}".format(headers[0], headers[1], headers[2]), [[headers[0], "Movies"], [headers[1], tname.strip("Movie_")]]]

   elif(tname == "Genres"):
      m1 = get_max_field_size(tname, headers[-1])
      return [["INT"] + ["VARCHAR({0})".format(m1)], headers[0], []]

   elif(tname == "Companies"):
      m1 = get_max_field_size(tname, headers[1])
      m2 = get_max_field_size(tname, headers[2])
      return [["INT"] + ["VARCHAR({0})".format(m1), "VARCHAR({0})".format(m2)], headers[0], []]

   elif(tname == "Crew" or tname == "Actors"):
      m1 = get_max_field_size(tname, headers[0])
      m2 = get_max_field_size(tname, headers[2])
      m3 = get_max_field_size(tname, headers[-3])
      m4 = get_max_field_size(tname, headers[-2])
      return [["VARCHAR({0})".format(m1), "INT", "VARCHAR({0})".format(m2)] + ["DATE"]*2 + ["INT"] + ["VARCHAR({0})".format(m3), "VARCHAR({0})".format(m4)] + ["DOUBLE"], headers[1], []]

###returns the script as a text for creating a table###
def create_table_script(tname, cnames, ctypes, pkey, fkeys):
   script = "CREATE TABLE {0} (".format(tname)
   for cname, ctype in zip(cnames, ctypes):
      script += "{0} {1}, ".format(cname, ctype)
   script += "PRIMARY KEY ({0}), ".format(pkey)
   if(tname == "Movies"):
      script += "FULLTEXT idx (overview), "
   for fkey in fkeys:
      script += "FOREIGN KEY ({0}) REFERENCES {1}({0}), ".format(fkey[0], fkey[1])
   return script[:-2] + ');'

###creates a table and commits the changess to the database###
def create_table(tname, cnames, ctypes, pkey, fkey):
   script = create_table_script(tname, cnames, ctypes, pkey, fkey)
   cursor.execute(script)
   conn.commit()
   print("SUCCESFULLY CREATED TABLE {0}".format(tname))

###connects to the database and defines the cursor###
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

###creates the tables, if the code failed to create anytable it deletes all###
###the tables in the database. I commented the deletion so the database don't###
###get deleted by mistake, at the end closes connection with the database###
if __name__ == "__main__":
   connect_to_db()
   tables = ["Movies", "Genres", "Companies", "Movie_Genre", "Movie_Company", "Crew", "Actors", "Movie_Crew", "Movie_Actors"]
   try:
      for table in tables:
         types = get_fields_types(table)
         create_table(table, get_headers(table), types[0], types[1], types[2])
   except Exception as e:
      to_del_tables = ["Movie_Genre", "Movie_Company", "Movie_Crew", "Movie_Actors", "Genres", "Companies", "Crew", "Actors", "Movies"]
      '''
      for table in to_del_tables:
         try:
            cursor.execute("DROP TABLE {0}".format(table))
         except:
            continue
      '''
      print("TABLE CREATION FAILED - {0}".format(e))
   cursor.close()
   conn.close()


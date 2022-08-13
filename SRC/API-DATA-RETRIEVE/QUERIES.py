###IN THIS CODE THERE ARE 7 FUNCTIONS, EACH ONE EXECUTES###
###THE RELEVANT QUERY###

import mysql.connector as connector
cursor, conn = None, None

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

###each function of the 7 functions executes the query with the right arguments###

def exec_query1():
    cursor.execute("SELECT c.cast_name, c.cast_id, COUNT(mc.movie_id)  \
                    FROM Actors AS c, Movie_Actors AS mc  \
                    WHERE c.known_for_department = 'Acting' AND c.cast_id = mc.cast_id  \
                    GROUP BY c.cast_name, c.cast_id  \
                    ORDER BY COUNT(mc.movie_id) DESC  \
                    LIMIT 1000")
    return cursor.fetchall()


def exec_query2():
    cursor.execute("SELECT g.genre_name AS genre, AVG(m.division) AS budget_over_revenue  \
                    FROM (SELECT mo.movie_id AS id, (mo.budget / mo.revenue) AS division  \
                          FROM Movies AS mo) AS m, Movie_Genre AS mg, Genres AS g  \
                    WHERE m.id = mg.movie_id AND mg.genre_id = g.genre_id  \
                    GROUP BY g.genre_name  \
                    ORDER BY AVG(m.division)")
    return cursor.fetchall()


def exec_query3():
    '''
    cursor.execute("CREATE VIEW temp AS  \
                    SELECT g.genre_name AS x, c.company_name AS y, COUNT(m.movie_id) AS z  \
                    FROM Genres AS g, Movie_Genre AS mg, Movies AS m, Movie_Company AS mc, Companies AS c  \
               	    WHERE g.genre_id = mg.genre_id AND mg.movie_id = m.movie_id AND m.movie_id = mc.movie_id AND mc.company_id = c.company_id  \
               	    GROUP BY g.genre_name, c.company_name")
    '''
    cursor.execute("SELECT temp.x, temp.y, temp.z  \
                    FROM temp  \
                    WHERE (temp.x, temp.z) IN (SELECT temp.x, MAX(temp.z) FROM temp Group By temp.x)  \
                    ORDER BY temp.x")
    return cursor.fetchall()


def exec_query4(word):
    cursor.execute("SELECT movie_title  \
                    FROM Movies  \
                    WHERE MATCH(overview) AGAINST('{0}')".format(word))
    return cursor.fetchall()


def exec_query5(actor_name):
    cursor.execute("SELECT g.genre_name, AVG(m.vote_average) AS average_rating  \
                    FROM (SELECT * FROM Actors AS cs WHERE cs.cast_name = '{0}' AND cs.known_for_department = 'Acting') AS c,  \
                    Movie_Actors AS mc, Movies AS m, Movie_Genre AS mg, Genres AS g  \
                    WHERE c.cast_id = mc.cast_id AND mc.movie_id = m.movie_id  \
                    AND m.movie_id = mg.movie_id AND mg.genre_id = g.genre_id  \
                    GROUP BY g.genre_name  \
                    HAVING AVG(m.vote_average) > 6  \
                    ORDER BY AVG(m.vote_average) DESC".format(actor_name))
    return cursor.fetchall()


def exec_query6(genre):
    cursor.execute("SELECT m.movie_title AS title, m.vote_average AS rating  \
                    FROM (SELECT * FROM Movies AS mo  \
                          WHERE mo.vote_count > 7000) AS m, Movie_Genre AS mg, \
                                (SELECT * FROM Genres AS ge WHERE ge.genre_name = '{0}') AS g  \
                    WHERE m.movie_id = mg.movie_id AND mg.genre_id = g.genre_id  \
                    ORDER BY m.vote_average DESC  \
                    LIMIT 10".format(genre))
    return cursor.fetchall()


def exec_query7(genre):
    cursor.execute("SELECT c.cast_name AS name, COUNT(m.movie_id) AS num_of_movies_in_genre  \
                    FROM (SELECT * FROM Actors AS ca WHERE ca.known_for_department = 'Acting' AND ca.popularity > 1.3) AS c,  \
                          Movie_Actors AS mc, Movie_Genre AS mg, (SELECT * FROM Genres AS ge WHERE ge.genre_name = '{0}') AS g,  \
                          (SELECT * FROM Movies AS mo WHERE mo.vote_average > 6.5) AS m  \
                    WHERE mg.genre_id = g.genre_id AND m.movie_id = mg.movie_id AND mc.movie_id = m.movie_id AND c.cast_id = mc.cast_id  \
                    GROUP BY c.cast_name  \
                    ORDER BY COUNT(m.movie_id) DESC  \
                    LIMIT 100".format(genre))
    return cursor.fetchall()

if __name__ == "__main__":
    try:
       connect_to_db()
    except:
       print("Error connecting to the database")
       ###should return Error try again later to the user###
       
    ###here should be a listener that receives any https request from a user and deals with it###
    ###htq holds the query number requested by the user, retreived from the http request###
    htq = 3
    try:
       if(htq > 3):
          ###htarg holds the query argument passed by the user, retreived from the http request###
          htarg = "Action"
       if(htq == 1):
          res = exec_query1()
       elif(htq == 2):
          res = exec_query2()
       elif(htq == 3):
          res = exec_query3()
       elif(htq == 4):
          res = exec_query4(htarg)
       elif(htq == 5):
          res = exec_query5(htarg)
       elif(htq == 6):
          res = exec_query6(htarg)
       elif(htq == 7):
          res = exec_query7(htarg)
    except:
       print("Error executing the query")
       ###should return Error try again later to the user###
    #else:
       #here we should send error page (bad httprequest) to the user#
    #here we should send the results hold in res as an httpresponse to the user#
    

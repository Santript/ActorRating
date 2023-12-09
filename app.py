from flask import Flask, request, jsonify, render_template
import pymysql
import uuid
import datetime
import time

app = Flask(__name__)

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'Firebubble123'
app.config['MYSQL_DB'] = 'actor_rating'

mysql = pymysql.connect(
    host = app.config['MYSQL_HOST'],
    user = app.config['MYSQL_USER'],
    password= app.config['MYSQL_PASSWORD'],
    db = app.config['MYSQL_DB']
)

def addInformation(firstName, lastName, movieTitle, genre, rating, review):
    #add the actor first and last name
    actor_id = addActor(firstName, lastName)

    #add the movie and movie genre
    movie_id = addMovie(movieTitle, genre)
    #add information to Actor_Movie table
    addActorMovie(actor_id, movie_id)

    #add the rating and review to Ratings table
    addRating(rating, review, actor_id)

def addActor(firstName, lastName):
    actor_id = str(uuid.uuid4())
    try:
        cursor = mysql.cursor()

        #prepared statement
        actorsTable = "insert into Actors values(\'" + actor_id + "\',\'" + firstName + "\',\'" + lastName + "\');"
        cursor.execute(actorsTable)
        mysql.commit()
        results = cursor.fetchall()

        for row in results:
            print(row)

        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))
    
    return actor_id

def addMovie(movieTitle, genre):
    movie_id = str(uuid.uuid4())
    try:
        cursor = mysql.cursor()

        moviesTable = "insert into Movies values(\'" + movie_id + "\',\'" + movieTitle + "\',\'" + genre + "\');"
        cursor.execute(moviesTable)
        mysql.commit()
        results = cursor.fetchall()

        for row in results:
            print(row)

        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))
    
    return movie_id

def addActorMovie(a_id, m_id):
    actor_movie_id = str(uuid.uuid4())
    
    try:
        cursor = mysql.cursor()

        # actor_movie_proc = "CREATE PROCEDURE InsertActorMovie(IN p_actor_movie_id VARCHAR(50),IN p_actor_id VARCHAR(50),IN p_movie_id VARCHAR(50)\n BEGIN\n INSERT INTO Actor_Movie VALUES (p_actor_movie_id, p_actor_id, p_movie_id);\n END;"
        # print()
        # print(actor_movie_proc)
        # print()

        actor_movie_table = "insert into Actor_Movie values(\'" + actor_movie_id + "\',\'" + a_id + "\',\'" + m_id + "\');"

        cursor.execute(actor_movie_table)
        mysql.commit()

        print("Stored Procedure has been created!")
        #cursor.callproc('InsertActorMovie', [actor_movie_id,a_id,m_id])

        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

user_id = str(uuid.uuid4())
def addRating(rating, review, actor_id):
    try:
        cursor = mysql.cursor()

        rating_id = str(uuid.uuid4())
        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        reviewTable = "insert into Ratings values(\'" + rating_id + "\',\'" + actor_id + "\',\'" + user_id + "\'," + rating + ",\'" + review + "\',\'" + timestamp + "\');"

        cursor.execute(reviewTable)
        mysql.commit()
        results = cursor.fetchall()

        for row in results:
            print(row)

        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

def showRowsToDelete():
    try:
        cursor = mysql.cursor()

        reviewTable = "select * from Ratings where user_id=\'9122bbf7-26cd-49e4-bf64-72a29a329f6c\';"

        cursor.execute(reviewTable)
        mysql.commit()
        results = cursor.fetchall()
        an = []

        resultsList = [list(i) for i in results]
        for i in range(len(resultsList)):
            actorname = "select first_name,last_name from actors where actor_id=\'" + resultsList[i][1] + "\';"
            cursor.execute(actorname)
            mysql.commit()
            an.append(cursor.fetchall())
            resultsList[i].pop(0)
            resultsList[i].pop(0)
            resultsList[i].pop(0)

        cursor.close()
        
        anList = [list(i) for i in an]
        for i in range(len(resultsList)):
            resultsList[i].insert(0, anList[i][0][0] + " " + anList[i][0][1])

        return resultsList
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

def deleteActors(actorNames):
    try:
        cursor = mysql.cursor()

        for actor in actorNames:

            delAct = "delete from Ratings where user_id=\'9122bbf7-26cd-49e4-bf64-72a29a329f6c\' and actor_id=(select actor_id from actors where first_name=\'" + actor.split()[0] + "\' and last_name=\'" + actor.split()[1] + "\');"

            cursor.execute(delAct)
            mysql.commit()
        
        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

@app.route('/insert')
def run():
    return render_template("insert.html")

@app.route('/insert_submit', methods=["POST", "GET"])
def parseDataInsert():
    if request.method == "POST":
        actorName = request.form.get("Actor")
        movieName = request.form.get("Movie")
        genre = request.form.get("Genre")
        firstName = actorName.split()[0]
        lastName = actorName.split()[1]
        rating = request.form.get("Rating")
        review = request.form.get("review")

        addInformation(firstName, lastName, movieName, genre, rating, review)
        
    return render_template("insert.html")

@app.route('/delete', methods=["POST", "GET"])
def parseDeleteData():
    res = showRowsToDelete()
    #print(res)
    return render_template("delete.html", res=res)

@app.route('/delete_submit', methods=["POST", "GET"])
def queryDelete():
    if request.method == "POST":
        deleteNames = request.form.get("actorDel")
        deleteActors(deleteNames.split(','))

    #res = showRowsToDelete()
    return render_template("insert.html")

if __name__ == '__main__':
    app.run(debug=False)
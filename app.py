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

        moviesTableProc = """
        CREATE PROCEDURE InsertMovie(
            IN movie_id VARCHAR(50),
            IN movieTitle VARCHAR(50),
            IN genre VARCHAR(50)
        )
        BEGIN
            INSERT INTO movies
            VALUES (movie_id, movieTitle, genre);
        END;
        """
        cursor.execute(moviesTableProc)

        moviesTable = "CALL InsertMovie(\'" + movie_id + "\', \'" + movieTitle + "\', \'" + genre + "\');"

        cursor.execute(moviesTable)
        cursor.execute("DROP PROCEDURE InsertMovie;")

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

        actor_movie_proc = """
        CREATE PROCEDURE InsertActorMovie(
            IN p_actor_movie_id VARCHAR(50),
            IN p_actor_id VARCHAR(50),
            IN p_movie_id VARCHAR(50)
        )
        BEGIN
            INSERT INTO Actor_Movie
            VALUES (p_actor_movie_id, p_actor_id, p_movie_id);
        END;
        """

        cursor.execute(actor_movie_proc)

        #actor_movie_table = "insert into Actor_Movie values(\'" + actor_movie_id + "\',\'" + a_id + "\',\'" + m_id + "\');"
        actor_movie_table = "CALL InsertActorMovie(\'" + actor_movie_id + "\', \'" + a_id + "\', \'" + m_id + "\');"

        cursor.execute(actor_movie_table)
        cursor.execute("DROP PROCEDURE InsertActorMovie;")
        mysql.commit()

        #print("Stored Procedure has been created!")
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

        #reviewTable = "select * from Ratings where user_id=\'" + user_id +"\';"
        reviewTable = "select * from Ratings where user_id=\'984088f9-fd95-4593-b9af-cfdffdad60df\';"

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
            #984088f9-fd95-4593-b9af-cfdffdad60dfs
            #delAct = "delete from Ratings where user_id=\'" + user_id + "\' and actor_id=(select actor_id from actors where first_name=\'" + actor.split()[0] + "\' and last_name=\'" + actor.split()[1] + "\');"
            delAct = "delete from Ratings where user_id=\'984088f9-fd95-4593-b9af-cfdffdad60df\' and actor_id=(select actor_id from actors where first_name=\'" + actor.split()[0] + "\' and last_name=\'" + actor.split()[1] + "\');"

            cursor.execute(delAct)
            mysql.commit()
        
        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

def updateRows(updates):
    try:
        cursor = mysql.cursor()
        updateQuery = ""
        updatesAttr = updates.split(",")

        if updatesAttr[1] == "Rating":
            updateQuery = "update Ratings set " + updatesAttr[1] + "=" + updatesAttr[2] + " where user_id=\'984088f9-fd95-4593-b9af-cfdffdad60df\' and timestamp=\'" + updatesAttr[0] + "\';"
        elif updatesAttr[1] == "Review":
            updateQuery = "update Ratings set " + updatesAttr[1] + "=\'" + updatesAttr[2] + "\' where user_id=\'984088f9-fd95-4593-b9af-cfdffdad60df\' and timestamp=\'" + updatesAttr[0] + "\';"
        else:
            updateQuery = "update actors set first_name=\'" + updatesAttr[2].split()[0] + "\', last_name=\'" + updatesAttr[2].split()[1] + "\' where actor_id=(select actor_id from Ratings where timestamp=\'" + updatesAttr[0] + "\');"


        cursor.execute(updateQuery)
        mysql.commit()

        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

#select rating,review from ratings where actor_id=(select actor_id from actors where first_name='' and last_name='');
def reportActor(actorName):
    try:
        cursor = mysql.cursor()
        #CREATE INDEX ON RATING
        sqlCreateSecondaryIndex1 = """CREATE INDEX firstNameIndex ON actors(first_name);"""
        sqlCreateSecondaryIndex2 = """CREATE INDEX lastNameIndex ON actors(last_name);"""
        cursor.execute(sqlCreateSecondaryIndex1)
        cursor.execute(sqlCreateSecondaryIndex2)

        reportActorQuery = "select actor_id,rating,review,timestamp from ratings where actor_id=(select actor_id from actors where first_name=\'" + actorName.split()[0] + "\' and last_name=\'" + actorName.split()[1] + "\');"

        cursor.execute(reportActorQuery)
        mysql.commit()

        results = cursor.fetchall()
        an = []

        resultsList = [list(i) for i in results]
        for i in range(len(resultsList)):
            actorname = "select first_name,last_name from actors where actor_id=\'" + resultsList[i][0] + "\';"
            cursor.execute(actorname)
            mysql.commit()
            an.append(cursor.fetchall())
            resultsList[i].pop(0)

        anList = [list(i) for i in an]
        for i in range(len(resultsList)):
            resultsList[i].insert(0, anList[i][0][0] + " " + anList[i][0][1])

        cursor.execute("DROP INDEX firstNameIndex ON actors;")
        cursor.execute("DROP INDEX lastNameIndex ON actors;")
        cursor.close()
        return resultsList
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

#select * from ratings r join actor_movie am on r.actor_id=am.actor_id join movies m on am.movie_id=m.movie_id where m.title='';
def reportMovie(movieName):
    try:
        cursor = mysql.cursor()
        #CREATE INDEX ON RATING
        sqlCreateSecondaryIndex = """CREATE INDEX movieTitleIndex ON movies(title);"""
        cursor.execute(sqlCreateSecondaryIndex)

        reportMovieQuery = "select r.actor_id,r.rating,r.review,m.title,m.genre,r.timestamp from ratings r join actor_movie am on r.actor_id=am.actor_id join movies m on am.movie_id=m.movie_id where m.title=\'" + movieName +"\';"

        cursor.execute(reportMovieQuery)
        mysql.commit()

        results = cursor.fetchall()

        resultsList = [list(i) for i in results]
        an = []

        for i in range(len(resultsList)):
            actorname = "select first_name,last_name from actors where actor_id=\'" + resultsList[i][0] + "\';"
            cursor.execute(actorname)
            mysql.commit()
            an.append(cursor.fetchall())
            resultsList[i].pop(0)

        anList = [list(i) for i in an]
        for i in range(len(resultsList)):
            resultsList[i].insert(0, anList[i][0][0] + " " + anList[i][0][1])

        cursor.execute("DROP INDEX movieTitleIndex ON movies;")
        cursor.close()
        return resultsList
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))

#select * from ratings where rating > <val>;
def reportRating(ratingVal):
    try:
        cursor = mysql.cursor()

        #CREATE INDEX ON RATING
        sqlCreateSecondaryIndex = """CREATE INDEX ratingIndex ON ratings(rating);"""
        cursor.execute(sqlCreateSecondaryIndex)


        ratingQuery = "select r.actor_id,r.rating,r.review,m.title,m.genre,r.timestamp from ratings r join actor_movie am on r.actor_id=am.actor_id join movies m on am.movie_id=m.movie_id where r.rating >= " + ratingVal +";"

        cursor.execute(ratingQuery)
        mysql.commit()

        results = cursor.fetchall()

        resultsList = [list(i) for i in results]
        an = []

        for i in range(len(resultsList)):
            actorname = "select first_name,last_name from actors where actor_id=\'" + resultsList[i][0] + "\';"
            cursor.execute(actorname)
            mysql.commit()
            an.append(cursor.fetchall())
            resultsList[i].pop(0)

        anList = [list(i) for i in an]
        for i in range(len(resultsList)):
            resultsList[i].insert(0, anList[i][0][0] + " " + anList[i][0][1])

        cursor.execute("DROP INDEX ratingIndex ON ratings;")

        cursor.close()
        return resultsList
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

@app.route('/update', methods=["POST", "GET"])
def updateQuery():
    res = showRowsToDelete()
    return render_template("update.html", res=res)

@app.route('/update_submit', methods=["POST", "GET"])
def updateQueryDone():
    if request.method == "POST":
        updates = request.form.get("updates")
        updateRows(updates)
    return render_template("insert.html")

@app.route('/report', methods=["POST", "GET"])
def report():
    return render_template("report.html")

@app.route('/report_submit', methods=["POST", "GET"])
def reportSubmit():
    actorInfo = []
    movieInfo = []
    ratingInfo = []

    if request.method == "POST":
        if request.form.get("Actor") is not None:
            actorInfo = reportActor(request.form.get("Actor"))
        elif request.form.get("Movie") is not None:
            movieInfo = reportMovie(request.form.get("Movie"))
        else:
            ratingInfo = reportRating(request.form.get("Rating"))
    
    # print(actorInfo)
    # print(movieInfo)
    # print(ratingInfo)
    return render_template("report.html", actorInfo=actorInfo, movieInfo=movieInfo, ratingInfo=ratingInfo)



if __name__ == '__main__':
    app.run(debug=False)
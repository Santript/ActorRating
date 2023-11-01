from flask import Flask, request, jsonify, render_template
import pymysql

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

def addActor(firstName, lastName):
    try:
        cursor = mysql.cursor()

        cursor.execute("SELECT actor_id FROM Actors ORDER BY actor_id DESC LIMIT 1")
        mysql.commit()
        prev_id = cursor.fetchone()

        if prev_id is None:
            prev_id = 1
        else:
            prev_id = prev_id[0] + 1

        #prepared statement
        actorsTable = "insert into Actors values(" + str(prev_id) + ",\'" + firstName + "\',\'" + lastName + "\');"
        cursor.execute(actorsTable)
        mysql.commit()
        results = cursor.fetchall()

        for row in results:
            print(row)

        cursor.close()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))
    
def addMovie(movieTitle, genre):
    try:
        cursor = mysql.cursor()

        cursor.execute("SELECT movie_id FROM Movies ORDER BY movie_id DESC LIMIT 1")
        mysql.commit()
        prev_id = cursor.fetchone()

        if prev_id is None:
            prev_id = 1
        else:
            prev_id = prev_id[0] + 1
        
        moviesTable = "insert into Movies values(" + str(prev_id) + ",\'" + movieTitle + "\',\'" + genre + "\');"
        cursor.execute(moviesTable)
        mysql.commit()
        results = cursor.fetchall()

        for row in results:
            print(row)

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

        addActor(firstName, lastName)
        addMovie(movieName, genre)
        
    return render_template("insert.html")


if __name__ == '__main__':
    app.run(debug=False)
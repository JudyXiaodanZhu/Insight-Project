# -*- coding: utf-8 -*-
import os
from flask import Flask, request, flash, redirect, render_template, url_for, abort, jsonify
try:
    from urllib.parse import urlparse, urljoin
except ImportError:
     from urlparse import urlparse, urljoin
from flask_login import LoginManager, login_user, logout_user, current_user, login_required
from passlib.hash import pbkdf2_sha256
import database
from werkzeug.utils import secure_filename
from cassandra.cluster import Cluster
import config
import subprocess
from itertools import product
from cassandra.concurrent import execute_concurrent_with_args
import plotly
import plotly.plotly as py
import plotly.tools as tls
import plotly.graph_objs as go
import pandas as pd
from datetime import date, datetime
import numpy as np
import json


# initialize the app, thumbnail, db and s3
app = Flask(__name__)
app.config.from_pyfile('config.cfg')
database.init_db(app)
db = database.db

# set login manager parameters
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = u"Please login to access this page."
from model import Users
from forms import RegistrationForm, LoginForm

#set db parameters
cluster = Cluster(config.cass_cluster_IP)
session = cluster.connect('ecg')


@app.route('/')
def index():
    """ If the current user is authenticated, directly route to dashboard."""
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    return render_template('index.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Validates the login parameters by checking the db and the pre-set validators."""
    form = LoginForm(request.form)
    if request.method == 'POST' and form.validate():
        next_var = request.args.get('next')
        user = Users.query.get(form.email.data)
        if user:
            # sets the authenticated parameter which is needed for sessions to recognize the user
            user.authenticated = True
            db.session.add(user)
            db.session.commit()
            login_user(user, remember=True)
        return redirect(next_var or url_for('home'))
    return render_template('login.html', form=form, email=request.cookies.get('email'))


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Registers the user and sets a hashed password."""
    form = RegistrationForm(request.form)
    if request.method == 'POST' and form.validate():
        hash_var = pbkdf2_sha256.encrypt(form.password.data, rounds=200000, salt_size=16)
        user = Users(form.email.data, hash_var)
        db.session.add(user)
        db.session.commit()
        login_user(user, remember=True)
        flash('User Registered')
        return redirect(url_for('home'))
    return render_template('register.html', form=form)


@app.route("/logout", methods=["GET"])
@login_required
def logout():
    """Logout the current user."""
    user = current_user
    user.authenticated = False
    db.session.add(user)
    db.session.commit()
    logout_user()
    return redirect(url_for('index'))


@app.route('/dashboard')
@login_required
def home():
    """Displays the data."""

    cql_str1 = '''SELECT * FROM display LIMIT 10''' 
    df_tbl_1 = list(session.execute(cql_str1))
    for i in range(len(df_tbl_1)):
        temp = (str(df_tbl_1[i][0]),)
        temp += df_tbl_1[i][8],
        for j in range(len(df_tbl_1[i])):
            if j == 8 or j == 0: continue
            temp +=  df_tbl_1[i][j],
        df_tbl_1[i] = temp
    header = ['Patient Id', 'Irregularity', 'Age',  'BMI', 'BSA', 'EF',  'Gender', 'Height', 'IMT', 'MALVMi','SBP', 'SBV', 'Smoker', 'Vascular_event','Weight']
    
    cql_str2 = '''SELECT count(*) as count FROM display''' 
    df_tbl_2 = list(session.execute(cql_str2))
    pending = ''
    for i in range(len(df_tbl_2)):
        pending = df_tbl_2[i][0]
    return render_template('dashboard.html', header=header, data=df_tbl_1, pending=pending)


@app.route('/dashboardtest')
@login_required
def test():
    """Displays the data."""

    cql_str1 = '''SELECT * FROM display LIMIT 10''' 
    df_tbl_1 = list(session.execute(cql_str1))
    header = ['Patient Id', 'Irregularity', 'Age',  'BMI', 'BSA', 'EF',  'Gender', 'Height', 'IMT', 'MALVMi','SBP', 'SBV', 'Smoker', 'Vascular_event','Weight']
    cql_str2 = '''SELECT count(*) as count FROM display''' 
    df_tbl_2 = list(session.execute(cql_str2))
    pending = ''
    for i in range(len(df_tbl_2)):
        pending = df_tbl_2[i][0]

    return jsonify(header=header, data=df_tbl_1, pending = pending)

@app.route('/call', methods=['POST'])
def call():
    response = subprocess.Popen(["./call.sh"])
    print(response.returncode)
    return redirect(url_for('home'))

@app.route('/search', methods=['GET', 'POST'])
def search_file():
    graph = []
    """Search function."""
    if request.method == 'POST':
        user = int(request.form['userID'])
        days = []
        # datetime.strptime(request.form['DateFrom'], "%B %d, %Y").date()
        if request.form['DateTo'] == request.form['DateFrom']:
            days = [request.form['DateFrom'].encode('utf-8')]
        else:
            days = [request.form['DateFrom'].encode('utf-8'), request.form['DateTo'].encode('utf-8')]
        prepared = session.prepare("Select message from ecg_stream where Record = ? and day = ? limit 10")
        args = product([user], days)
        for i in args:
            print i
        """
        results = execute_concurrent_with_args(session, prepared, args)
        for (success, result) in results:
            if not success:
                return render_template("search.html")
            else:
                if len(result.current_rows) > 0:
                    graph = process_result(result)
        """
        results = session.execute(prepared, (user,days[0]))
        if len(results.current_rows) > 0:
            graph = process_result(results)
    return render_template("search.html", trace1_data=graph)


def process_result(result):
    df_tbl_1 = pd.DataFrame(list(result))
    res = []
    
    for num in df_tbl_1['message']:
        num = num.encode('utf-8').split(' ')
        for number in num:
            if isfloat(number):
                if float(number) < 1.0:
                    res.append(float(number))
    time = range(len(res))
    trace1 = dict(
            x= time,
            y=res,
            name="re",
            type='lines',
            )
   
    return json.dumps(trace1, cls=plotly.utils.PlotlyJSONEncoder)


def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False


@login_manager.user_loader
def user_loader(user_id):
    """Given *user_id*, return the associated User object.
    :param unicode user_id: user_id (email) user to retrieve
    """
    return Users.query.get(user_id)


if __name__ == "__main__":
    # execute only if run as a script
    app.run(host='0.0.0.0',port=5001)

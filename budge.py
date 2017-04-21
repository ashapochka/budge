from pprint import pprint
from functools import wraps, update_wrapper
from datetime import datetime

from flask import Flask, render_template, redirect, url_for, request, \
    make_response
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from wtforms import SubmitField
from wtforms.fields.html5 import IntegerField


def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = make_response(view(*args, **kwargs))
        response.headers['Last-Modified'] = datetime.now()
        response.headers[
            'Cache-Control'] = 'no-store, no-cache, must-revalidate, ' \
                               'post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    return update_wrapper(no_cache, view)


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///local-data/budge.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = 'mysecretkey'

db = SQLAlchemy(app)


class SessionEntity(db.Model):
    __tablename__ = 'session'
    delay = db.Column(db.Float())
    duration = db.Column(db.Float())
    invite_time = db.Column(db.DateTime(), primary_key=True)
    user1 = db.Column(db.BigInteger(), primary_key=True)
    user2 = db.Column(db.BigInteger())
    end_time = db.Column(db.DateTime())
    response_code = db.Column(db.BigInteger())


class QueryForm(FlaskForm):
    user_id = IntegerField(label='User Id')
    submit = SubmitField()


@app.route('/')
@nocache
def home():
    pprint(request)
    form = QueryForm(formdata=request.args)
    user_id_value = form.user_id.data
    print(user_id_value)
    sessions = SessionEntity.query.limit(100).all()
    return render_template('home.html', form=form, sessions=sessions)


@app.route('/query', methods=['POST'])
def query():
    form = QueryForm()
    user_id_value = form.user_id.data
    print(user_id_value)
    return redirect(url_for('home', user_id=user_id_value))


if __name__ == '__main__':
    app.run(host='0.0.0.0')

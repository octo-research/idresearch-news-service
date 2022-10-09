from GoogleNews import GoogleNews
from flask import Flask, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS

app = Flask(__name__)
limiter = Limiter(app, key_func=get_remote_address)

def getNews(keyword):
    googlenews = GoogleNews()
    googlenews = GoogleNews(lang='id', region='ID', period='1d')
    googlenews.get_news(keyword)
    return googlenews.results()

cors = CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/news')
@limiter.limit("30/minute")
def hello_name():
    args = request.args
    keyword =  args.get('search')
    return getNews(keyword)

if __name__ == '__main__':
    app.run(port=3000, host='0.0.0.0')

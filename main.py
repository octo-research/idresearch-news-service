from pymongo import MongoClient
import requests
import os
import dataclasses
import time
from dataclasses import dataclass


level = {
    1:"provinsi",
    2:"kabupaten",
    3:"provinsi",
    4:"kabupaten"
}

@dataclass
class InformationLake:
    title: str
    description: str
    abstract: str
    author: str
    year: int
    daerah_label: str
    daerah_level: int
    daerah_code: int
    links: list
    topik_id: str
    source: str

def mongoDbConn():

    CONNECTION_STRING = "mongodb://admin:54535251@54.255.139.218/doajs?authSource=admin"

    client = MongoClient(CONNECTION_STRING)

    return client['doajs']

def get_params():
    response = requests.post("http://10.43.194.147:3000/v1/scraper/parameters", data = {"label": os.getenv("SCRAPER_NAME") })
    if response.status_code == 200:
        return response.json()['data']
    else:
        return None

def ingest_information(information):
    params = dataclasses.asdict(information)
    #print(json.dumps(params))
    response = requests.post("http://10.43.194.147:3000/v1/informations",  json = params)
    print(response.json())


while True:
    params = get_params()
   # Get the database
    dbname = mongoDbConn()
    #print(dbname)
    searchTerm =  "\"{}\"".format(level[params['daerah_level']].lower() + " " + params['daerah_label'].lower())
    print(searchTerm)
    time.sleep(2)
    for doc in dbname.article.find({ '$text': { '$search': searchTerm } }):
        print(doc['bibjson']['title'])
        doc_id = ""
        try:
            doc_id=doc['_id']
            authors = []
            for x in doc['bibjson']['author']:
                authors.append(x["name"])
            linksList = []
            for x in doc['bibjson']['link']:
                linksList.append(x['url'])
            information = InformationLake(
                title=doc['bibjson']['title'],
                description =doc['bibjson']['abstract'],
                abstract = '',
                author = ",".join(authors),
                year = int(doc['bibjson']['year']),
                daerah_label = params['daerah_label'],
                daerah_level = params['daerah_level'],
                daerah_code = params['daerah_code'],
                links = linksList,
                topik_id = "85747949-ab6f-4a2d-8370-01884d43a4b8",
                source = "Doaj",
            )
            #print(dataclasses.asdict(information))
            ingest_information(information)
        except Exception as e:
            print("err", e)
            dbname.article.deleteOne( {"_id": ObjectId( doc_id )})

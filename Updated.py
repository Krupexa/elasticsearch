from elasticsearch import Elasticsearch, helpers
from collections import deque
from lxml import etree
import datetime
import gzip
import warnings
import csv
import pandas as pd

def gendata():
    input = gzip.open('/Users/apple/PycharmProjects/PubMed/pubmed21n0036.xml.gz', 'r')
    tree = etree.parse(input)
    root = tree.getroot()

    for i in root:

            for id in root.iter('PMID'):
                 PMID= id.text

            Year = []
            for year in i.findall("./PubmedArticle/MedlineCitation/DateCompleted/Year"):
                if year != None:
                    Year.append(year.text)
            #print(Year)

            Month = []
            for month in i.findall("./PubmedArticle/MedlineCitation/DateCompleted/Month"):
                if Month != None:
                    Month.append(month.text)
            ##print(Month)

            Day = []
            for day in i.findall("./PubmedArticle/MedlineCitation/DateCompleted/Day"):
                if Day != None:
                    Day.append(day.text)
            #print(Day)

            for (a,b,c) in zip(Year,Month,Day):
                    PublishDate = datetime.datetime(int(a), int(b), int(c)).strftime("%Y-%m-%d")

            MeshIDs = [mesh.attrib['UI'] for mesh in i.iter('DescriptorName')]

            for title in root.iter('ArticleTitle'):
                    Titles = title.text

            LastName = []
            for LN in i.iter("PubmedArticle/MedlineCitation/Article/AuthorList/Author/LastName"):
                 if LN != None:
                    LastName.append(LN.text)
                    #print("LastName:", LastName)

            ForeName = []
            for FN in i.findall("PubmedArticle/MedlineCitation/Article/AuthorList/Author/ForeName"):
                 if FN != None:
                    ForeName.append(FN.text)
                     #print("ForeName:", ForeName)
            Authors = [(str(FN)+' '+str(LN)) for (FN,LN) in zip(ForeName,LastName)]

            #Abstract text
            for abstract in root.iter('AbstractText'):
                Abstract = abstract.text

            #List of keywords
            Keywords = [key.text for key in i.iter('Keywords')]

            #Uploader
            Uploader = "Krupexa Nakrani"

            yield ({"PMID": PMID,
                   "_ID" : PMID,
                    #"PublishDate": PublishDate,
                    "MeshIDs": MeshIDs,
                    "Titles": Titles,
                    "Authors": Authors,
                    "Abstract": Abstract,
                    "Keywords": Keywords,
                    "Uploader": Uploader})

if __name__ == '__main__':
    index_name = 'pubmed2021'
    #gendata()
    es = Elasticsearch(hosts=['10.80.34.86:9200'], http_auth=('elastic', 'iYYX96TPlAJ000UJ0vqa'))
    deque(helpers.parallel_bulk(es, gendata(), index=index_name), maxlen = 0)
    es.indices.refresh()




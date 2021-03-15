import csv
import json
import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3
import requests
import datetime
import re
from bs4 import BeautifulSoup
from urllib.request import Request
from urllib.request import urlopen
from six.moves import urllib

titulos = []
categorias = []
noticias = []

titulos2 = []
categorias2 = []
noticias2 = []
link=[]

s3 = boto3.client('s3')
fecha = datetime.datetime.now()
anio = fecha.year
print(anio)
mestexto = fecha.strftime("%B")
mes = fecha.month
dia = fecha.day

urllib.request.urlretrieve('https://www.eltiempo.com/', '/home/ubuntu/eltiempo.html')


ruta_Archivo = "/home/ubuntu/eltiempo.html"
s3.upload_file(ruta_Archivo,'scbucket',f'headlines/raw/periodico=eltiempo/year={anio}/month={mestexto}/day={dia}/eltiempo.html')
f = open(ruta_Archivo,'r')
Fe =  f.read()

soup = BeautifulSoup(Fe, 'lxml')


categories = soup.findall(class=re.compile("category page-link"))

titles = soup.findall(class="title page-link")

for i in titles:
    titulos.append(i.string)
    noticias.append([i.string,'', 'https://www.eltiempo.com/' + i['href']])

for i in range(len(categories)):
    categorias.append(categories[i].string)
    noticias[i][1] = categories[i].string

with open('/home/ubuntu/Eltiempo.csv','w',newline='') as fp:
    Q = csv.writer(fp,delimiter=',')
    Q.writerows(noticias)
s3.upload_file('/home/ubuntu/Eltiempo.csv','scbucket',f'headlines/final/periodico=eltiempo/year={anio}/month={mestexto}/day={dia}/Eltiempo.csv')

urllib.request.urlretrieve('https://www.publimetro.co/co/', '/home/ubuntu/publimetro.html')
ruta_Archivo = "/home/ubuntu/publimetro.html"
s3.upload_file(ruta_Archivo,'scbucket',f'headlines/raw/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.html')

ruta_Archivo2 = "/home/ubuntu/publimetro.html"
p = open(ruta_Archivo2,'r')
Pe = p.read()
soup2 = BeautifulSoup(Pe, 'lxml')

categories2 = soup2.findall(class=re.compile("cardKicker"))
for i in soup2.find_all('a',href=True):
    link.append(i['href'])

titles2 = soup2.findall(class=re.compile("cardTitle"))

for i in titles2:
    titulos2.append(i.string)
    noticias2.append([i.string,'',''])

for i in range(len(categories2)):
    categorias2.append(categories2[i].string)
    noticias2[i][1] = categories2[i].string

for i in range(len(titles2)):
    noticias2[i][2] = link[i]

for i in noticias2:
    print(i)

with open('/home/ubuntu/publimetro.csv','w',newline='') as fp2:
    Q2 = csv.writer(fp2,delimiter=',')
    Q2.writerows(noticias2)

s3.upload_file('/home/ubuntu/publimetro.csv','scbucket',f'headlines/final/periodico=publimetro/year={anio}/month={mestexto}/day={dia}/publimetro.csv')
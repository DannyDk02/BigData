import json
import os
import sys
import uuid
from urllib.parse import unquote_plus
import boto3
import requests
import datetime
from bs4 import BeautifulSoup
from urllib.request import Request
from urllib.request import urlopen
from six.moves import urllib
import csv

fecha = datetime.datetime.now()
anio = fecha.year
print(anio)
mestexto = fecha.strftime("%B")
mes = fecha.month
dia = fecha.day

athena = boto3.client('athena',region_name = 'us-east-1')

response = athena.start_query_execution(
    QueryString="alter table acciones add partition(company='avianca');",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString="alter table acciones add partition(company='ecopetrol');",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)
response = athena.start_query_execution(
    QueryString="alter table acciones add partition(company='cementosargos');",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
    )

response = athena.start_query_execution(
    QueryString="alter table acciones add partition(company='grupoaval');",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table acciones add partition(year={anio});",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table acciones add partition(month='{mestexto}');",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)
response = athena.start_query_execution(
    QueryString = f"alter table acciones add partition(day={dia});",
    QueryExecutionContext={
        'Database': 'scrapingweb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table periodicos add partition(periodico='eltiempo');",
    QueryExecutionContext={
        'Database': 'newsscraping'
    },
    ResultConfiguration={
'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table periodicos add partition(periodico='publimetro');",
    QueryExecutionContext={
        'Database': 'newsscraping'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table periodicos add partition(year={anio});",
    QueryExecutionContext={
        'Database': 'newsscraping'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)
response = athena.start_query_execution(
    QueryString = f"alter table periodicos add partition(month='{mestexto}');",
    QueryExecutionContext={
        'Database': 'newsscraping'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

response = athena.start_query_execution(
    QueryString = f"alter table periodicos add partition(day={dia});",
    QueryExecutionContext={
        'Database': 'newsscraping'
    },
    ResultConfiguration={
        'OutputLocation': 's3://scbucket/stocks/respuestas/',
        'EncryptionConfiguration': {
            'EncryptionOption': 'SSE_S3',
        }
    },
)

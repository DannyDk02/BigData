import os
import boto3
import requests
import datetime
import yfinance as yf


fecha = datetime.datetime.now()
year = fecha.year
T = fecha.strftime("%B")
month = fecha.month
day = fecha.day



data_df = yf.download("AVHOQ", start="{}-{}-{}".format(year, month, day-2), end="{}-{}-{}".format(year,month,day))
data_df.to_csv('AVHOQ.csv')
s3 = boto3.client('s3')
ruta_Archivo = f"/home/ubuntu/AVHOQ.csv"
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'scbucket','stocks/company=avianca/year={}/month={}/day={}/AVHOQ.csv'.format(year,T,day))


data_df = yf.download("CMTOY", start="{}-{}-{}".format(year, month, day-3), end="{}-{}-{}".format(year,month,day))
data_df.to_csv('CMTOY.csv')
s3 = boto3.client('s3')
ruta_Archivo = f"/home/ubuntu/CMTOY.csv"
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'scbucket','stocks/company=cementosargos/year={}/month={}/day={}/CMTOY.csv'.format(year,T,day))


data_df = yf.download("EC", start="{}-{}-{}".format(year, month, day-2), end="{}-{}-{}".format(year,month,day))
data_df.to_csv('EC.csv')
s3 = boto3.client('s3')
ruta_Archivo = f"/home/ubuntu/EC.csv"
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'scbucket',f'stocks/company=ecopetrol/year={year}/month={T}/day={day}/EC.csv')


data_df = yf.download("AVAL", start="{}-{}-{}".format(year, month, day-2), end="{}-{}-{}".format(year,month,day))
data_df.to_csv('AVAL.csv')
s3 = boto3.client('s3')
ruta_Archivo = f"/home/ubuntu/AVAL.csv"
lines = open(ruta_Archivo,'r').readlines()
lines[0] = ''
out = open(ruta_Archivo,'w')
out.writelines(lines)
out.close()
s3.upload_file(ruta_Archivo,'scbucket',f'stocks/company=grupoaval/year={year}/month={T}/day={day}/AVAL.csv')

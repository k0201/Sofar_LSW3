import mysql.connector
import requests
import configparser
from dateutil.parser import parse

def getActualFromSolcast(url,apikey):
    response = requests.get(url, auth=(apikey,''))
    if 200==response.status_code:
        solcastjson=response.json()
    pv_estimate=[]
    period_end=[]
    for element in solcastjson['estimated_actuals']:
        pv_estimate.append(element['pv_estimate'])
        period_end.append(str(parse(element['period_end']).timestamp()))
    rows = []
    for x in range(len(period_end)):
        rows.append((period_end[x],pv_estimate[x]))
    return rows
def getForecastFromSolcast(url,apikey):
    response = requests.get(url, auth=(apikey,''))
    if 200==response.status_code:
        solcastjson=response.json()
    pv_estimate=[]
    pv_estimate10=[]
    pv_estimate90=[]
    period_end=[]
    for element in solcastjson['forecasts']:        
        pv_estimate.append(element['pv_estimate'])
        pv_estimate10.append(element['pv_estimate10'])
        pv_estimate90.append(element['pv_estimate90'])
        period_end.append(str(parse(element['period_end']).timestamp()))
    rows = []
    for x in range(len(period_end)):
        rows.append((period_end[x],pv_estimate[x],pv_estimate10[x],pv_estimate90[x]))
    return rows

# CONFIG
configParser = configparser.RawConfigParser()
configFilePath = r'./config.cfg'
configParser.read(configFilePath)

dbcon = mysql.connector.connect(
      host=configParser.get('Database', 'db_host'),
      user=configParser.get('Database', 'db_user'),
      password=configParser.get('Database', 'db_password'),
      database=configParser.get('Database', 'db_name')
    )


responseActual = getActualFromSolcast(configParser.get('Solcast', 'actual_url'),configParser.get('Solcast', 'api_key'))
responseForecast = getForecastFromSolcast(configParser.get('Solcast', 'forecast_url'),configParser.get('Solcast', 'api_key'))

cursor = dbcon.cursor()
sql = "INSERT INTO solcast (time, pv_estimate) VALUES (%s,%s) ON DUPLICATE KEY UPDATE pv_estimate = VALUES(pv_estimate);"
cursor.executemany(sql, responseActual)
sql = "INSERT INTO solcast_forecast (time, pv_estimate ,pv_estimate10, pv_estimate90) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE pv_estimate = VALUES(pv_estimate),pv_estimate10 = VALUES(pv_estimate10),pv_estimate90 = VALUES(pv_estimate90);"
cursor.executemany(sql, responseForecast)
dbcon.commit()
cursor.close()
dbcon.close()


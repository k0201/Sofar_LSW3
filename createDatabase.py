import mysql.connector
import configparser

# CONFIG
configParser = configparser.RawConfigParser()
configFilePath = r'./config.cfg'
configParser.read(configFilePath)

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

databaseName=configParser.get('Database', 'db_name')
dbcon = mysql.connector.connect(
      host=configParser.get('Database', 'db_host'),
      user=configParser.get('Database', 'db_user'),
      password=configParser.get('Database', 'db_password')
    )
cursor = dbcon.cursor()
#Chceck if db exists and create it 
sql = "CREATE DATABASE IF NOT EXISTS %s" % (databaseName) 
cursor.execute(sql)
sql = "USE %s" % (databaseName) 
cursor.execute(sql)
#Chceck if table exist
if not checkTableExists(dbcon,"solcast"):
    cursor.execute("CREATE TABLE solcast (time INT UNSIGNED PRIMARY KEY, pv_estimate FLOAT)") 
if not checkTableExists(dbcon,"solcast_forecast"):
    cursor.execute("CREATE TABLE solcast_forecast (time INT UNSIGNED PRIMARY KEY, pv_estimate FLOAT,pv_estimate10 FLOAT,pv_estimate90 FLOAT)") 
if not checkTableExists(dbcon,"sofar"):
    sql = "CREATE TABLE sofar (timestamp INT UNSIGNED PRIMARY KEY , Inverter_status ENUM('Stand-by','Self-checking','Normal','FAULT','Permanent') , Fault_1 VARCHAR(255) , Fault_2 VARCHAR(255) , Fault_3 VARCHAR(255) , Fault_4 VARCHAR(255) , Fault_5 VARCHAR(255) , PV1_Voltage_V DECIMAL (6,2),PV1_Current_A DECIMAL (6,2),PV2_Voltage_V DECIMAL (6,2),PV2_Current_A DECIMAL (6,2),PV1_Power_W DECIMAL (6,2) ,PV2_Power_W DECIMAL (6,2) ,Output_active_power_W DECIMAL (6,2) ,Output_reactive_power_kVar DECIMAL (6,2) ,Grid_frequency_Hz DECIMAL (6,2) ,L1_Voltage_V DECIMAL (6,2) ,L1_Current_A DECIMAL (6,2) ,L2_Voltage_V DECIMAL (6,2) ,L2_Current_A DECIMAL (6,2) ,L3_Voltage_V DECIMAL (6,2) ,L3_Current_A DECIMAL (6,2) ,Total_production_kWh DECIMAL (6,2) ,Total_generation_time_h DECIMAL (6,2) ,Today_production_Wh DECIMAL (6,2) ,Today_generation_time_min DECIMAL (6,2) ,Inverter_module_temperature_C DECIMAL (6,2) ,Inverter_inner_termperature_C DECIMAL (6,2) ,Inverter_bus_voltage_V DECIMAL (6,2) ,PV1_voltage_sample_by_slave_CPU_V DECIMAL (6,2) ,PV1_current_sample_by_slave_CPU_A DECIMAL (6,2) ,Countdown_time_s DECIMAL (6,2) ,Inverter_alert_message DECIMAL (6,2) ,Input_mode DECIMAL (6,2) ,Communication_Board_inner_message DECIMAL (6,2) ,Insulation_of_PV1p_to_ground DECIMAL (6,2) ,Insulation_of_PV2p_to_ground DECIMAL (6,2) ,Insulation_of_PVm_to_ground DECIMAL (6,2) ,Country VARCHAR(255) ,String_1_voltage_V DECIMAL (6,2) ,String_1_current_A DECIMAL (6,2) ,String_2_voltage_V DECIMAL (6,2) ,String_2_current_A DECIMAL (6,2) ,String_3_voltage_V DECIMAL (6,2) ,String_3_current_A DECIMAL (6,2) ,String_4_voltage_V DECIMAL (6,2) ,String_4_current_A DECIMAL (6,2) ,String_5_voltage_V DECIMAL (6,2) ,String_5_current_A DECIMAL (6,2) ,String_6_voltage_V DECIMAL (6,2) ,String_6_current_A DECIMAL (6,2) ,String_7_voltage_V DECIMAL (6,2) ,String_7_current_A DECIMAL (6,2) ,String_8_voltage_V DECIMAL (6,2) ,String_8_current_A DECIMAL (6,2));"
    cursor.execute(sql) 
    
cursor.close()
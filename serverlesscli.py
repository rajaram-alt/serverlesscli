import sys
import os
import secrets
import csv
import mysql.connector
from datetime import date
import subprocess
import shutil
import json
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import sys

# lsof -i :5004  
# kill -9 11564 

app = Flask(__name__)
scheduler = BackgroundScheduler()


def input_need(input1):   
    with open(input1, 'r') as file:
        data = json.load(file)
        input_need= data
        return input_need

def folder_create(repo_url, destination_path):
    try:
        subprocess.run(['git', 'clone', repo_url, destination_path], check=True)
    except subprocess.CalledProcessError as e:
        pass
        # print(f"Error cloning repository: {e}")



# need=os.listdir(data["folder_path"])[1]

############################# read_db ################################# 
def readdb(data):
    host = data['db']['host']
    user = data['db']['user']
    password = data['db']['password']
    database = data['db']['database']
    folder_path = data['folder_path']
    read_tb = data['input_table']
    db_config = {
            "host":host,
            "user":user,
            "password":password,
            "database":database
        }

    def handle_csv_date(obj):
        if isinstance(obj, date):
            return obj.isoformat()
        else:
            raise TypeError("Type not serializable")

    connection = mysql.connector.connect(**db_config)
    try:
        with connection.cursor(dictionary=True) as cursor:
            query = f"SELECT * FROM {read_tb}"
            cursor.execute(query)
            rows = cursor.fetchall()
            # Specify the file paths
            csv_file_path = f'{folder_path}/{os.listdir(data["folder_path"])[1]}/first_output.csv'
            with open(csv_file_path, 'w', newline='') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys())
                csv_writer.writeheader()
                csv_writer.writerows(rows)
            print(f'Data has been written')

    finally:
        connection.close()

############################# write_db ################################# 
def writedb(data):
    try:
        host = data['db']['host']
        user = data['db']['user']
        password = data['db']['password']
        database = data['db']['database']
        folder_path = data['folder_path']

        db_config = {
            "host":host,
            "user":user,
            "password":password,
            "database":database
        }
        csv_file_path = f'{folder_path}/{os.listdir(data["folder_path"])[1]}/final.csv'
        new_table_name = data['result_table']+str(datetime.datetime.now()).replace('-',"_").replace(" ","_").replace(":","_").replace(".","_")
        connection = mysql.connector.connect(**db_config)

        try:
            # Create a cursor object to execute SQL queries
            with connection.cursor() as cursor:
                # Read the first row from the CSV file to get column names
                with open(csv_file_path, 'r') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    header = next(csv_reader)

                # Create a new table with dynamic schema
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {new_table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        {', '.join(f'{col} VARCHAR(255)' for col in header)}
                    )
                '''
                cursor.execute(create_table_query)

                # Read data from CSV and insert into the new table
                with open(csv_file_path, 'r') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        insert_query = f'''
                            INSERT INTO {new_table_name} ({', '.join(row.keys())})
                            VALUES ({', '.join(['%(' + col + ')s' for col in row.keys()])})
                        '''
                        cursor.execute(insert_query, row)
                connection.commit()

                print(f'Data has been inserted into the {new_table_name} table')

        finally:
            connection.close()
    except Exception as e:
        print({"error": f"An error occurred: {str(e)}"}), 500  # Internal Server Error status code


############################# runoutputlog ################################# 

def runoutputlog(data,input1):
    # script_name = request.json['script_name']
    folder_key = data['folder_path']
    folder=f'{folder_key}/{os.listdir(data["folder_path"])[1]}'
    process = subprocess.Popen(['python3', f'{folder}/{input1}.py'], stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate(input = folder)
    print(stdout)
    print(stderr)

############################# scheduled_task ################################# 
def scheduled_task(data):
    readdb(data)
    runoutputlog(data,'process_one')
    runoutputlog(data,'process_two')
    runoutputlog(data,'process_three')
    writedb(data)
    print(f'Task executed at {datetime.datetime.now()}')

# scheduler.add_job(scheduled_task, 'interval', hours=2)
# scheduler.add_job(scheduled_task, 'interval', minutes=30)
# scheduler.add_job(scheduled_task, 'interval', days=1)
    


def time_schedule(data,need):
    main=need.split('=')
    if main[0] == '--hour':
        scheduler.add_job(lambda: scheduled_task(data), 'interval', hours=int(main[1]))
        scheduler.start()
    elif main[0] == '--minute':
        scheduler.add_job(lambda: scheduled_task(data), 'interval', minutes=int(main[1]))
        scheduler.start()
    elif main[0] == '--day':
        scheduler.add_job(lambda: scheduled_task(data), 'interval', days=int(main[1]))
        scheduler.start()
    elif main[0] == '--second':
        main = [data]
        scheduler.add_job(lambda: scheduled_task(data), 'interval', seconds=int(main[1]))
        scheduler.start()


#############################  ################################# ################################# 
        
def main():
    if sys.argv[2] == 'read_db':
        data=input_need(sys.argv[1])
        folder_create("https://github.com/rajaram-alt/DataEngOperation.git",data["folder_path"])
        readdb(data)

    elif sys.argv[2] == 'write_db':
        data=input_need(sys.argv[1])
        folder_create("https://github.com/rajaram-alt/DataEngOperation.git",data["folder_path"])
        writedb(data) 

    elif sys.argv[2] == "run":
        data=input_need(sys.argv[1])
        folder_create("https://github.com/rajaram-alt/DataEngOperation.git",data["folder_path"])
        runoutputlog(data,sys.argv[3])

    elif sys.argv[2] == "schedule":
        data=input_need(sys.argv[1])
        folder_create("https://github.com/rajaram-alt/DataEngOperation.git",data["folder_path"])
        time_schedule(data,sys.argv[3])
        app.run(debug=True,port=sys.argv[4])

if __name__ == '__main__':
    main()

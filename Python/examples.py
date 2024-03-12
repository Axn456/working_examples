import requests, json
import pandas as pd
from pyodbc import connect


def GetAccessToken(username, password):
    url = 'xxxxxxxxxxxxxxxxxxxxxxxx'
    headers = {
        'accept': 'text/plain',
        'Content-Type': 'application/json-patch+json' 
    }
    data =  {
            "username": username,
            "password": password,
            "logging_mode": 'true',
            "device": "string",
            "app_version": "string",
            "ttl": 0
            }
    r = requests.post(url=url, headers=headers, json=data)
    return r.json()['data']['access_token']
    

def pallet (query):
    url = 'xxxxxxxxxxxxxxxxxxxxxxxx'
    headers = {
                'accept': 'application/json',
                'Content-Type': 'application/json'  
                }

    connection = connect('''Driver={SQL Server Native Client 11.0};
                            Server=Server;
                            Database=Database;
                            Trusted_Connection=yes;'''
                        )

    with connection.cursor() as cursor:
        cursor.execute(query)

        data = []
        for row in cursor.fetchall():
            payload = {
                "transfer_pallet_ids": list(row)
                    }
            r = requests.post(url, json=payload, headers=headers)
            data.append(r.json()['data']['pallets'][0])
        with open('data.json', 'w') as outfile:
            json.dump(data, outfile, indent=4 )        
    return pd.read_json('data.json').to_excel("output.xlsx")


def WarehouseCreate (warehouseid, name, accesstoken, typewh='Unknown'):       
    url = 'xxxxxxxxxxxxxxxxxxxxxx' 
    headers = {'accept': 'text/plain',
            'Authorization': f'Bearer {accesstoken}',
            'Content-Type': 'application/json-patch+json' 
            }
    payload =  {
                "id": warehouseid,
                "name": name,
                "is_active": True,
                "type": typewh
                }
    r = requests.post(url=url, headers=headers, json=payload)
    if not r.ok:
        print(f'Ошибка {r.text}') 
    print(f'Создан склад ID:{warehouseid} {name} type:{typewh}')   


def PLDCto200 (query):
    url = 'xxxxxxxxxxxxxxxxxx'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }

    connection = connect('''Driver={SQL Server Native Client 11.0};
                                Server=Server;
                                Database=Database;
                                Trusted_Connection=yes;'''
                            )

    with connection.cursor() as cursor:
            cursor.execute(query)
      
            for pldc , externalentityid, lozonid , srcPlaceId , dstPlaceId in cursor.fetchall(): 
                payload = {
                            "externalId": externalentityid,
                            "containers": [
                                {
                                "containerId": lozonid,
                                "srcPlaceId": srcPlaceId,
                                "dstPlaceId": dstPlaceId
                                }
                            ],
                            "resolveNotDeparted": False
                        }  
        
                r = requests.post(url=url, headers=headers, json=payload)  
                if r.status_code == 400:
                    print(f'Ошибка {pldc} {r.text}')  
                else:             
                    print(f'Выполнено {pldc}')          


def PLDCto100 (query):
    url = 'xxxxxxxxxxx'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }

    connection = connect('''Driver={SQL Server Native Client 11.0};
                                Server=Server;
                                Database=Database;
                                Trusted_Connection=yes;'''
                            )

    with connection.cursor() as cursor:
            cursor.execute(query)
      
            for pldc , externalentityid, lozonid , srcPlaceId , dstPlaceId in cursor.fetchall(): 
                payload = {
                            "externalId": externalentityid,
                            "containers": [
                                {
                                "containerId": lozonid,
                                "srcPlaceId": srcPlaceId,
                                "dstPlaceId": dstPlaceId
                                }
                            ]
                        }  
        
                r = requests.post(url=url, headers=headers, json=payload)  
                if r.status_code == 400:
                    print(f'Ошибка {pldc} {r.text}')  
                else:             
                    print(f'Выполнено {pldc}')  


    
       
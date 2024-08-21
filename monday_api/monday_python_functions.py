
import requests
import json
import pandas as pd

def process_monday_board_values(df,board_id):
    output = pd.DataFrame()
    for i,j in df.iterrows():
        row = {}
        row['truck'] = j['name']
        for data in j['column_values']:
            row_id = data['id']
            text = data['text']
            row[row_id] = text
        output = pd.concat([output, pd.DataFrame([row])], ignore_index=True)
    output.insert(0,'monday_board_id',board_id)
    return output

def list_monday_boards_with_keypath(api_keypath):

    with open(api_keypath, 'r') as f:
        auth = json.load(f)

    api_key = auth['key']
    apiUrl = "https://api.monday.com/v2"
    headers = {"Authorization" : api_key}

    query = '{ boards (limit:1000) {name id} }'
    data = {'query' : query}
    r = requests.post(url=apiUrl, json=data, headers=headers) # make request

    df = pd.json_normalize(r.json()['data']['boards'])

    return df

def list_monday_boards_with_keystring(api_key_string):

    apiUrl = "https://api.monday.com/v2"
    headers = {"Authorization" : api_key_string}

    query = '{ boards (limit:1000) {name id} }'
    data = {'query' : query}
    r = requests.post(url=apiUrl, json=data, headers=headers) # make request

    df = pd.json_normalize(r.json()['data']['boards'])

    return df

def get_monday_data(board_id, apiUrl, headers):

    complete = False
    page = 1
    full_output = pd.DataFrame()
    while complete == False:
        query =   f'''{{ 
        boards (ids: {board_id}, page:{page}) {{
        name
        id
        description
        items_page (limit:500){{
            items {{
            name
            column_values {{
            id
            type
            text
        }} }} }} }} }}'''

        data = {'query' : query}
        r = requests.post(url=apiUrl, json=data, headers=headers) # make request

        df = pd.json_normalize(r.json()['data']['boards'][0]['items_page']['items'])

        output = process_monday_board_values(df,board_id)
        full_output = pd.concat([full_output, output], ignore_index=True)

        if len(output) == 500:
            page += 1
        else:
            complete = True
    return full_output

def get_monday_data_with_keypath(api_key_path, board_id,):
    # '../keys/api_key.json'
    with open(api_key_path, 'r') as f:
        auth = json.load(f)

    api_key = auth['key']
    apiUrl = "https://api.monday.com/v2"
    headers = {"Authorization" : api_key}

    output = get_monday_data(board_id,apiUrl,headers)

    return output

def get_monday_data_with_keystring(api_key_string, board_id):

    apiUrl = "https://api.monday.com/v2"
    headers = {"Authorization" : api_key_string}

    output = get_monday_data(board_id,apiUrl,headers)

    return output
# Blaseball api endpoint archive. 
from modernblaseball.modern_blaseball import blaseball_api
import boto3, json, time, csv

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_boto3_client():
    return boto3.client('s3')

# downloads object from s3 and decodes it. I authenticated using local .aws folder files. 
def get_file(client, bucket, key):
    file_obj = client.get_object(Bucket=bucket, Key=key)
    return file_obj['Body'].read().decode('utf-8')


# writing the file so that I can take a look at it. 
def write_file(file_contents, file_name):
    file_out = open(file_name, 'w')
    file_out.write(file_contents)
    file_out.close()

# replaces the log file in s3 with one that you have scrubbed.  an S3 PUT will overwrite existing files. 
def upload_file(client, bucket, key, file_contents):
    client.put_object(Body=str.encode(file_contents), Bucket=bucket, Key=key)


# list objects to check what is available.
def list_objects(client, bucket): 
    print(client.list_objects(Bucket=bucket))

def write_page_to_files(csv_content, label):
    print('writing csv file')
    f = open(label, "w")
    dict_writer = csv.DictWriter(f, csv_content[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(csv_content)
    f.close()

def flatten_teams(all_teams, all_players): 
    pass

# innefficient mess, but I promised someone in discord a CSV and I gotta MOVE 
def extract_player_ids(all_teams):
    players = []
    player_ids = []
    for team in all_teams:
        nick_name = team['nickname']
        for player_id in team['lineup']:
            players.append({'team_name': nick_name, 
                            'team_role': 'lineup',
                            'id': player_id})
            player_ids.append(player_id)
        for player_id in team['rotation']:
            players.append({'team_name': nick_name, 
                            'team_role': 'rotation',
                            'id': player_id})
            player_ids.append(player_id)
        for player_id in team['bullpen']:
            players.append({'team_name': nick_name, 
                            'team_role': 'bullpen',
                            'id': player_id})
            player_ids.append(player_id)
        for player_id in team['bench']:
            players.append({'team_name': nick_name, 
                            'team_role': 'bench',
                            'id': player_id})
            player_ids.append(player_id)

    return players, player_ids

def stitch_players(players, player_results):
    player_stats = []
    for page in player_results:
        player_stats += json.loads(page.text)
    
    player_stitch = []
    for player_team_data in players:
        matched_player = [player for player in player_stats if player_team_data['id'] in player['_id']]

        player_team_data.update(matched_player[0])
        player_stitch.append(player_team_data)

    return player_stitch

def manage_batch(endpoint_func, list_of_payloads, sleepy_time=1): 
    results = []
    
    for payload in list_of_payloads:
        results.append(blaseball.failover_500(endpoint_func, payload))
        time.sleep(sleepy_time)
    return results

blaseball = blaseball_api()

all_teams = blaseball.failover_500(blaseball.get_all_teams)

players, player_ids = extract_player_ids(json.loads(all_teams.text))

player_stats = blaseball.failover_500(blaseball.get_player_stats, player_ids)

chunked_list = chunks(player_ids, 50)

player_results = manage_batch(blaseball.get_player_stats, chunked_list, sleepy_time=2)

player_stat_list = stitch_players(players, player_results)

print(player_stat_list)

write_page_to_files(player_stat_list, 'full_player_data.csv')

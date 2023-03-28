import requests
import json
import boto3
import urllib
import yaml
import sys
import os

def build_query(params):
  return('&'.join([f"{k}={v}" for k, v in params.items()]))

def rerun_workflow(branch):
  token = os.environ('GITHUB_TOKEN')
  owner = "TASUKI-Corporation"
  repo = "tasuki-funds"
  base_url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
  headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
  }
  params = {
    "per_page": 1,
    "status": "completed",
    "branch": branch
  }
  response = requests.get(f"{base_url}?{build_query(params)}", headers=headers)
  res_json = response.json()

  if len(res_json['workflow_runs']) > 0:
    run = res_json['workflow_runs'][0]
    response = requests.post(f"{base_url}/{run['id']}/rerun", headers=headers)

    if response.statu_code == 201:
      print(f"INFO: {branch}のワークフローを再実行しました")
    else:
      print(f"ERROR: {branch}のワークフローの再実行に失敗しました")
  else:
    print(f"INFO: {branch}の実行済みのワークフローがありません")

def lambda_handler(event, context):
  s3 = boto3.client('s3')
  
  for record in event['Records']:
    bucket = record['s3']['bucket']['name']
    key = urllib.parse.unquote(record['s3']['object']['key'])
    version_list = s3.list_object_versions(Bucket=bucket, Prefix=key)['Versions']

    if len(version_list) > 1:
      latest_version_id = version_list[1]['VersionId']
      previous_version = yaml.load(s3.get_object(Bucket=bucket, Key=key, VersionId=latest_version_id)['Body'])
      current_version = yaml.load(s3.get_object(Bucket=bucket, Key=key)['Body'])
      for stage in current_version:
        if current_version[stage] != previous_version[stage]:
          branch = 'master' if stage == 'production' else stage
          rerun_workflow(branch)

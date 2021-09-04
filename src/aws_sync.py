#!/usr/bin/env python
import boto3
import re
import glob
import os
import datetime
from dateutil import tz
import time

source_bucket = 'kuki-dev-bucket-01'
source_prefix = 'dev/'
target_bucket = 'kuki-dev-bucket-01'
target_prefix = 'prd/'

source_path = r'C:\temp\s3テストデータ'

#aws s3 sync s3://kuki-dev-bucket-01/dev/ s3://kuki-dev-bucket-01/prd/

def sync_s3_to_s3(source_bucket='', source_prefix='', target_bucket='', target_prefix='', dryrun=False, output=False):
    file_list_source = get_s3_file_list(source_bucket, source_prefix)
    file_list_target = get_s3_file_list(target_bucket, target_prefix)
    index_source:int = 0
    index_target:int = 0
    while True:
        # ファイル名が同じ場合、サイズ・日付が違う場合コピー
        if file_list_source[index_source]['file_name'] == file_list_target[index_target]['file_name']:
            if not (file_list_source[index_source]['file_size'] == file_list_target[index_target]['file_size']
                and file_list_source[index_source]['file_modified'] < file_list_target[index_target]['file_modified']) :
                if output or dryrun:
                    print('Copying: s3://' + source_bucket + '/' + source_prefix + file_list_source[index_source]['file_name'] + ' To s3://' + target_bucket + '/' + target_prefix + file_list_source[index_source]['file_name'] )
                if not dryrun:
                    s3client.copy_object(Bucket=target_bucket, Key=target_prefix + file_list_source[index_source]['file_name'], CopySource={'Bucket': source_bucket, 'Key': source_prefix + file_list_source[index_source]['file_name']})
            
            index_source += 1
            index_target += 1
        # コピー先にない場合、コピー
        elif file_list_source[index_source]['file_name'] < file_list_target[index_target]['file_name'] or file_list_target[index_target]['file_name'] == '':
            if output or dryrun:
                if not file_list_target[index_target]['file_name'].startswith(file_list_source[index_source]['file_name']):
                    print('Copying　New: s3://' + source_bucket + '/' + source_prefix + file_list_source[index_source]['file_name'] + ' To s3://' + target_bucket + '/' + target_prefix + file_list_source[index_source]['file_name'] )
            if not dryrun:
                s3client.copy_object(Bucket=target_bucket, Key=target_prefix + file_list_source[index_source]['file_name'], CopySource={'Bucket': source_bucket, 'Key': source_prefix + file_list_source[index_source]['file_name']})
            index_source += 1
        # コピー元にない場合、削除
        elif file_list_source[index_source]['file_name'] > file_list_target[index_target]['file_name'] or file_list_source[index_source]['file_name'] == '':
            if output or dryrun:
                print('delete: s3://' + target_bucket + '/' + target_prefix + file_list_target[index_target]['file_name'])
            if not dryrun:
                s3client.delete_object(Bucket=target_bucket, Key=target_prefix + file_list_target[index_target]['file_name'])
            index_target += 1
        if file_list_source[index_source]['file_name'] == '' and file_list_target[index_target]['file_name'] == '':
            break

def sync_local_to_s3(source_path='', target_bucket='', target_prefix='', dryrun=False, output=False):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(target_bucket)
    
    file_list_source = get_local_file_list(source_path)
    file_list_target = get_s3_file_list(target_bucket, target_prefix)
    index_source:int = 0
    index_target:int = 0
    kara_dir = ''
    while True:
        # ファイル名が同じ場合、サイズ・日付が違う場合コピー
        if file_list_source[index_source]['file_name'] == file_list_target[index_target]['file_name']:
            if not (file_list_source[index_source]['file_size'] == file_list_target[index_target]['file_size']
                and file_list_source[index_source]['file_modified'] < file_list_target[index_target]['file_modified']) :
                if file_list_source[index_source]['isDir']:
                    pass
                else:
                    if output or dryrun:
                        print('Copying: file://' + source_path + os.sep + file_list_source[index_source]['file_name'] + ' To s3://' + target_bucket + '/' + target_prefix + file_list_source[index_source]['file_name'] )
                    if not dryrun:
                        bucket.upload_file(source_path + os.sep + file_list_source[index_source]['file_name'], target_prefix + file_list_source[index_source]['file_name'])
            index_source += 1
            index_target += 1
        # コピー先にない場合、コピー
        elif file_list_source[index_source]['file_name'] < file_list_target[index_target]['file_name'] or file_list_target[index_target]['file_name'] == '':
            if file_list_source[index_source]['isDir']:
                if not dryrun:
                    if not file_list_target[index_target]['file_name'].startswith(file_list_source[index_source]['file_name']):
                        if output or dryrun:
                            print('Copying　New: file://' + source_path + os.sep + file_list_source[index_source]['file_name'] + ' To s3://' + target_bucket + '/' + target_prefix + file_list_source[index_source]['file_name'] )
                        s3client.put_object(Bucket=target_bucket, Key=target_prefix + file_list_source[index_source]['file_name'])
                    else:
                        kara_dir = file_list_source[index_source]['file_name']
            else:
                if output or dryrun:
                    print('Copying　New: file://' + source_path + os.sep + file_list_source[index_source]['file_name'] + ' To s3://' + target_bucket + '/' + target_prefix + file_list_source[index_source]['file_name'] )
                if not dryrun:
                    bucket.upload_file(source_path + os.sep + file_list_source[index_source]['file_name'], target_prefix  + file_list_source[index_source]['file_name'])
            index_source += 1
        # コピー元にない場合、削除
        elif file_list_source[index_source]['file_name'] > file_list_target[index_target]['file_name'] or file_list_source[index_source]['file_name'] == '':
            if output or dryrun:
                if not file_list_source[index_source]['file_name'].startswith(file_list_target[index_target]['file_name']):
                    print('delete: s3://' + target_bucket + '/' + target_prefix + file_list_target[index_target]['file_name'])
            if not dryrun:
                if not file_list_source[index_source]['file_name'].startswith(file_list_target[index_target]['file_name']):
                    s3client.delete_object(Bucket=target_bucket, Key=target_prefix + file_list_target[index_target]['file_name'])
                if not file_list_target[index_target+1]['file_name'].startswith(kara_dir) and not len(kara_dir) == 0:
                    s3client.put_object(Bucket=target_bucket, Key=target_prefix + kara_dir)
                    kara_dir = ''
            index_target += 1
        if file_list_source[index_source]['file_name'] == '' and file_list_target[index_target]['file_name'] == '':
            break

def get_s3_file_list(source_bucket='', source_prefix=''):
    contents_count = 0
    next_token = ''
    file_list = []
    while True:
        if next_token == '':
            response = s3client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        else:
            response = s3client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix, ContinuationToken=next_token)
        if 'Contents' in response:
            contents = response['Contents']
            contents_count = contents_count + len(contents)
        for content in contents:
            relative_prefix = re.sub('^' + source_prefix, '', content['Key'])
            if not relative_prefix == '':
                file_timestamp = content['LastModified']
                file_timestamp = file_timestamp.strftime("%Y-%m-%d %H:%M:%S")
                file_list += [{'file_name' : relative_prefix, 'file_size' : content['Size'], 'file_modified' : file_timestamp}]
        if 'NextContinuationToken' in response:
            next_token = response['NextContinuationToken']
        else:
            break
    file_list.sort(key=lambda val: val['file_name'])
    file_list += [{'file_name' : '', 'file_size' : '', 'file_modified' : ''}]
    return file_list

def get_local_file_list(source_path = ''):
    os.chdir(source_path)
    file_list = []
    for file_name in glob.glob("./**", recursive=True):
        file_name_relative = re.sub(r'^\.\\', '', file_name)
        file_name_relative = file_name_relative.replace('\\','/')
        if not file_name_relative == '':
            file_size = os.path.getsize(file_name)
            file_timestamp = os.path.getatime(file_name)
            file_timestamp = datetime.datetime.fromtimestamp(file_timestamp)
            file_timestamp = file_timestamp.astimezone(tz.gettz('UTC'))
            file_timestamp = file_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            if os.path.isfile(file_name):
                file_list += [{'file_name' : file_name_relative, 'file_size' : file_size, 'file_modified' : file_timestamp,'isDir' : False}]
            else:
                dir_file_count = glob.glob(file_name + os.sep + "**", recursive=True)
                if len(dir_file_count) == 1:
                    file_list += [{'file_name' : file_name_relative + '/', 'file_size' : file_size, 'file_modified' : file_timestamp,'isDir' : True}]
    file_list.sort(key=lambda val: val['file_name'])
    file_list += [{'file_name' : '', 'file_size' : '', 'file_modified' : '','isDir' : False}]
    return file_list

if __name__ == "__main__":
    s3client = boto3.client('s3')
    print('Local To S3')
    sync_local_to_s3(source_path, source_bucket, source_prefix, False, True)
    time.sleep(10)
    print('S3 To S3')
    sync_s3_to_s3(source_bucket, source_prefix, target_bucket, target_prefix, False, True)

    # print('S3 File List')
    # for file_obj in get_s3_file_list(source_bucket, source_prefix):
    #     print(file_obj)

    # print('local File List')
    # for file_obj in get_local_file_list(source_path):
    #     print(file_obj)

    # file_list_a = get_s3_file_list(source_bucket, source_prefix)
    # file_list_b = get_s3_file_list(target_bucket, target_prefix)
    # print(type(file_list_a))
    # print(file_list_a)
    # file_list_a.sort(key=lambda val: val['file_size'])
    # print(type(file_list_a))
    # print(file_list_a)

    # sortlist = file_list_a.sort()
    # sorted(file_list_a, key = lambda x:x[1])
    # print(sortlist)
    # file_list_a.sort(key = lambda x: x[1], reverse=True)
    # for file in file_list_a:
    #     print(file['file_name'] + '  --  ' + str(file['file_size']) + '  --  ' + str(file['file_modified']))
    
    # print('file_list_a : ' + str(file_list_a))
    # print('file_list_b : ' + str(file_list_b))
    # print ("Hello, Python")


# リスト取得 from

# リスト取得 to

# file チェック (サイズ、日付)

# 繰り返し

# 件数カウント
# コピーファイル　print

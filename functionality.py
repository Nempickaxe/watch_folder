#!/usr/bin/env python
# coding: utf-8
import os, shutil, time, dateparser, datetime

def get_time_accessed(path):
    return (time.time() - os.path.getatime(path))

def write_log(path, log_file):
    with open(log_file, 'a') as file:
        file_path_list = ['deleted file:\t', path, '\t|date deleted\t', datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), '\n']
        file.write(''.join(file_path_list))

def delete_file(path, log_file):
    os.remove(path)
    write_log(path, log_file)

def delete_non_empty_folder(path, log_file):
    shutil.rmtree(path, ignore_errors=True)
    write_log(path, log_file)

def delete_empty_folder(path, log_file):
    os.rmdir(path)
    write_log(path, log_file)

def check_file_if_collection(path):
    end_extensions = ['.parquet']
    for item in end_extensions:
        if path.endswith(item):
            return True
    return False

def get_any_file(path):
    return os.path.join(path, os.listdir(path)[0]) 

def master_delete(folder_path, time_threshold, log_file, check_if_master_empty=False):
    ''' takes input master directory and time in epochs'''
    first_dir_exist = True
    #if master folder exist
    while first_dir_exist:
        if not os.path.isdir(folder_path):
            print(folder_path, 'not a directory')
            return None
        else:
            first_dir_exist=False
    #delete folder if empty(optional)
    if check_if_master_empty:
        if (os.path.isdir(folder_path)):
            if os.listdir(folder_path)==[]:
                delete_empty_folder(folder_path, log_file)
                print('deleted empty folder', folder_path)
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        #if path is a file
        if (os.path.isfile(item_path)):
            if get_time_accessed(item_path)>=time_threshold:
                delete_file(item_path, log_file)
                print('deleted file: ', item_path)
        #if path is of type parquet
        elif check_file_if_collection(item_path):
                sample_file = get_any_file(item_path)
                if get_time_accessed(sample_file)>=time_threshold:
                    delete_non_empty_folder(item_path, log_file)
                    print('deleted collection type: ', item_path)
        #if empty directory
        elif os.listdir(item_path)==[]:
            delete_empty_folder(item_path, log_file)
            print('deleted empty folder', item_path)
        #if folder then recurssion
        else:
            master_delete(item_path, time_threshold, log_file, check_if_master_empty=True)

def get_epoch(x):
    return dateparser.parse(x).timestamp()

def watch_folders(folders_info, log_file):
    '''absolute paths of folders and natural language threshold'''
    with open(folders_info) as file:
        folder_threshold = file.readlines()[1:]
        for item in folder_threshold:
            folder, threshold = [i.strip() for i in (item.strip()).split(',')]
            print('WATCHING:\t', folder, '\nSET THRESHOLD:\t', threshold)
            #get epoch equivalent
            threshold = time.time()-get_epoch(threshold)
            master_delete(folder, threshold, log_file)

if __name__=='__main__':
    master_delete('test_files', 1500)
    
if __name__=='__main__':
    watch_folders('threshold_master.csv', 'logfile.txt')
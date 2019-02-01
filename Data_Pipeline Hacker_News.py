from pipeline import Pipeline
import csv
from pipeline import build_csv
import json
import io

import string
import re

from functools import partial

import datetime

from stop_words import stop_words

pipeline=Pipeline()

### Goal is to filter out stories that are most Popular and do a word count of the most popular words used:
#* Criteria: 50 points, more than 1 comment, and titles that do not begin with Ask HN

@pipeline.task()
def file_to_json():
    
    json_file=open('hn_stories_2014.json')
    
    output=json.load(json_file)
    
    return output['stories']

	@pipeline.task(depends_on=file_to_json)
def filter_stories(li):
    
    def inner(date):
        return datetime.datetime.strptime(date,'%Y-%m-%dT%H:%M:%SZ')
    
    generator=((a['title'],a['objectID'],inner(a['created_at']),a['url'],a['points']) for a in li if ((a['title']!='Ask HN')&(a['points']>=50)&(a['num_comments']>1)))
    
    
    return generator

@pipeline.task(depends_on=filter_stories)
def json_to_csv(filtered_dat):
    output=build_csv(filtered_dat,header=['title','objectID', 'created_at', 'url', 'points'],file=io.StringIO())
    return output


@pipeline.task(depends_on=json_to_csv)
def extract_titles(output):
    
    titles_gen=(a.split(',')[0] for a in output)
    
    return titles_gen

@pipeline.task(depends_on=extract_titles)
def clean_titles(input_titles):
    
    punct=string.punctuation
    punct=punct+'‘'+'’'
    re_exp='['+punct+']'
    
    clean_gen=(re.sub(re_exp,' ',a.lower()) for a in input_titles)

    return clean_gen

@pipeline.task(depends_on=clean_titles)
def build_keyword_dictionary(cleaned_titles_gen):
    
    partial_func=partial(lambda x: re.split(' *',x.strip()))
    
    
    keyword_dic={}
    
    def inner(x):
        if x!='title':
            li=partial_func(x)

            for x in li:
                if x not in stop_words:
                    if x not in keyword_dic.keys():
                        keyword_dic[x]=1
                    else:
                        keyword_dic[x]+=1

        return keyword_dic
    
    gen=(inner(title) for title in cleaned_titles_gen)
    
    for dic in gen:
        
        try:
            final_dic=dic
            
        except Exception as e:
            pass
        
    return  final_dic

@pipeline.task(depends_on=build_keyword_dictionary)
def sorting_dic(final_dic):
   
    final_dic1=sorted(final_dic.items(), key=lambda kv:kv[1],reverse=True)  
    
    return final_dic1
	
pipeline_output=pipeline.run()

print(pipeline_output.keys())

function_li=list(pipeline_output.keys())

# Prints out the sorted word count dictionary from the most used words to the least used

print(pipeline_output[function_li[6]])

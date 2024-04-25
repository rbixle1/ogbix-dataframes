#!/usr/bin/python -u
from stemming.porter2 import stem
import re
from html.parser import HTMLParser
from hashlib import blake2b

STOPWORDS = ['a','able','about','across','after','all','almost','also','am','among',
             'an','and','any','are','as','at','be','because','been','but','by','can',
             'cannot','could','dear','did','do','does','either','else','ever','every',
             'for','from','get','got','had','has','have','he','her','hers','him','his',
             'how','however','i','if','in','into','is','it','its','just','least','let',
             'like','likely','may','me','might','most','must','my','neither','no','nor',
             'not','of','off','often','on','only','or','other','our','own','rather','said',
             'say','says','she','should','since','so','some','than','that','the','their',
             'them','then','there','these','they','this','tis','to','too','twas','us',
             'wants','was','we','were','what','when','where','which','while','who',
             'whom','why','will','with','would','yet','you','your']

def stopWord(word, stopwords=STOPWORDS):
    if word in stopwords:
        return True
    return False
    
def remove(wordlist, stopwords=STOPWORDS):
    marked = []
    for t in wordlist:
        if stopWord(t):
            t = ''
        t =  re.sub(r"[^A-Za-z0-9]+", '', t)
        t = stem(t.strip())
        
        if len(t) > 0:
            marked.append(t)
    if len(marked) > 0:
        return marked
    else:
        return wordlist

def stemmer(item_to_stem):
    words = remove(item_to_stem.lower().split(' '))
    word = ''.join(words)
    return word

def key_builder(key):
    if isinstance(key, bytes): 
        key = key.decode(encoding='UTF-8')

    # Stem the key, remove stopwords, remove html tags, remove control chars
    key = stemmer(key)
    
    # Hash the key
    my_hash = blake2b(key=b'1968', digest_size=16)
    my_hash.update(key.encode('utf-8'))
    key = my_hash.hexdigest()
    return key

# This is just to run and debug the code.
key_builder('hello ran blah   some other junk  this world')
import hashlib
from .tokenizer import tokenize

# do a fingerprint
# for each page, we need to create a dictionary for {key: the page, value: fingerprint value}

def three_gram(text):
    words = tokenize(text)
    three_grams = []

    for i in range(len(words)-2):
        three_grams.append(''.join(words[i:i+3]))
    
    return three_grams

def hash_value(three_gram):
    return int(hashlib.md5(three_gram.encode('utf-8')).hexdigest(), 16)

def select_hash(hashes, k=50):
    return sorted(hashes)[:k]


# put all 3 processes together to create the fingerprint
def get_fp(text):
    three_grams = three_gram(text)
    hashes = [hash_value(g) for g in three_grams]
    fingerprint = select_hash(hashes)
    return fingerprint
def get_similarity(fp1, fp2): 
    set1, set2 = set(fp1), set(fp2)

    intersection = set1.intersection(set2)
    union = set1.union(set2)
    if not union:
        return 0
    return len(intersection) / len(union)

def is_similar_to_visited(current_fingerprint, visited_sites_fingerprint, threshold):
    for visited_fp in visited_sites_fingerprint:
        # calc the similarity score between current and visited fingerprints
        similarity = get_similarity(current_fingerprint, visited_fp)
        if similarity >= threshold: 
            return True
    return False
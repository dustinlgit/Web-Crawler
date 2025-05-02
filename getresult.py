import shelve

# Replace 'result' with your actual shelve filename (without the .db extension)
with shelve.open('scraper_results') as db:
    # Load and print each item
    unique_count = db.get('unique_count')
    print(f"Unique Count: {unique_count}")

    longest_page_url = db.get('longest_page_url')
    print(f"Longest Page URL: {longest_page_url}")

    longest_page_word_count = db.get('longest_page_word_count')
    print(f"Longest Page Word Count: {longest_page_word_count}")

    top50words = db.get('top50words')
    print("\nTop 50 Words:")
    if top50words:
        for word, count in top50words:
            print(f"{word}: {count}")

    subdomain_count = db.get('subdomain_count')
    print("\nSubdomain Counts:")
    if subdomain_count:
        for subdomain, count in subdomain_count.items():
            print(f"{subdomain}: {count}")

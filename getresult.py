import shelve
def show_result(filename="scraper_results", output_file="scraper_output.txt"):
    with shelve.open(filename) as db, open(output_file, "w", encoding="utf-8") as f:
        unique_count = db.get('unique_count')
        f.write(f"Unique Count: {unique_count}\n")
        print(f"Unique Count: {unique_count}")

        longest_page_url = db.get('longest_page_url')
        f.write(f"Longest Page URL: {longest_page_url}\n")
        print(f"Longest Page URL: {longest_page_url}")

        longest_page_word_count = db.get('longest_page_word_count')
        f.write(f"Longest Page Word Count: {longest_page_word_count}\n")
        print(f"Longest Page Word Count: {longest_page_word_count}")

        top50words = db.get('top50words')
        f.write("\nTop 50 Words:\n")
        print("\nTop 50 Words:")
        if top50words:
            for word, count in top50words:
                line = f"{word}: {count}"
                f.write(line + "\n")
                print(line)

        subdomain_count = db.get('subdomain_count')
        f.write("\nSubdomain Counts:\n")
        print("\nSubdomain Counts:")
        if subdomain_count:
            for subdomain in sorted(subdomain_count.keys()):
                count = subdomain_count[subdomain]
                line = f"{subdomain}: {count}"
                f.write(line + "\n")
                print(line)

if __name__ == "__main__":
    show_result()
import requests, time
from urls import websites

urls = websites.split("\n")
num_urls = len(urls)

start = time.time()

for url in urls:
    print(f"fetching url {url}")
    r = requests.get(url)

end = time.time()

print(f"Took {end - start} seconds to pull {num_urls} websites")
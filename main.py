import asyncio
import csv
import logging
import os
import re
import random

import shodan
from dotenv import load_dotenv

from evcs import garo, ensto
from evcs.custom_logger.logger import CustomFormatter

load_dotenv()
ColorfulHandler = logging.StreamHandler()
ColorfulHandler.setFormatter(CustomFormatter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        ColorfulHandler
    ]
)
logger = logging.getLogger(__name__)

# Reading the Shodan API key from .env file
SHODAN_API_KEY = os.environ.get("SHODAN_API_KEY")

# Initialize the Shodan API
api = shodan.Shodan(SHODAN_API_KEY)
semaphore = asyncio.Semaphore(9)  # Set the size of the connection pool

# Results will be stored in this list
results = []
# To be used in the evcs.garo module
garo_ip_addresses = []
ensto_ip_addresses = []

# The titles to search for
titles = [
    "GARO EVSE Status", "EVSE Status", "EVSE Configuration",
    "EVSE - SPECIFICATIONS", "EVSE - DASHBOARD", "Kempower Charging Station Panel",
    "Charging Station Management System", "Charging station web interface",
    "Charging station web interface :: Etrel", "EV Charging Station EV-Charger",
    "Charging station interface", "ArthEV - Charging Station",
    "EV Charging Station", "EV Cloud Administration Panel",
    "Amplicity - EV charging station"]


async def search_title(title):
    try:
        # Perform the search on Shodan
        query = f'title:"{title}"'
        result = await loop.run_in_executor(None, lambda: api.search(query))
        return result

    except shodan.APIError as e:
        logger.error(f"Error: {e}")
        return {'matches': []}


async def extract_data_async(service):
    website_title = service.get("http", {}).get('title', '')
    ip = service.get("ip_str", '')
    retries = 3  # Number of retries before giving up

    async with semaphore:
        for _ in range(retries):
            try:
                # Asynchronously get host information
                hostinfo = await loop.run_in_executor(None, lambda: api.host(ip))
                open_ports = [port for port in hostinfo.get('ports', [])]
                cves = service.get('vulns', {})
                # Convert CVEs dict to a list if not None
                cves_list = re.findall(r'CVE-\d{4}-\d{4,7}', str(cves.keys())) if cves else ["None"]
                # Extract the domains if they are available
                hostnames = ' - '.join(service.get('hostnames', []))
                # Append results to the list including domains
                results.append([ip, hostnames, open_ports, website_title, ' - '.join(cves_list)])

                logging.info(f"Collected Data from Shodan for: {ip}")

                # Check if the website title was 'GARO EVSE Status'
                if website_title == 'GARO EVSE Status':
                    garo_ip_addresses.append(ip)
                elif website_title == 'Charging station interface':
                    ensto_ip_addresses.append(ip)

                break  # Break the loop if successful

            except shodan.APIError as e:
                if 'Rate limit reached' in str(e):
                    # Rate limit reached, wait and retry
                    wait_time = random.randint(2,8)  # Exponential backoff
                    logging.warning(f"Rate limit reached. Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    # Other API errors, log and break the loop
                    logging.error(f"API Error: {e}")
                    break



async def main():
    # Use asyncio.gather for concurrent API calls
    search_titles_tasks = [search_title(title) for title in titles]
    search_results_list = await asyncio.gather(*search_titles_tasks)

    extract_data_tasks = [extract_data_async(service) for search_results in search_results_list for service in
                          search_results['matches']]
    await asyncio.gather(*extract_data_tasks)

    os.makedirs('results', exist_ok=True)

    # Run garo.run_scraper and ensto.run_scraper in parallel
    await asyncio.gather(
        loop.run_in_executor(None, lambda: garo.run_scraper(garo_ip_addresses)),
        loop.run_in_executor(None, lambda: ensto.run_scraper(ensto_ip_addresses))
    )


def write_data_to_csv():
    # Define the CSV file name
    output_filename = 'results/shodan_search_results.csv'

    # Write the results to a CSV file
    with open(output_filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write header row with 'Domains' added
        writer.writerow(["IP", "Hostnames", "Open Ports", "Title", "Known CVEs"])
        writer.writerows(results)

    logger.info(f"Data has been saved to '{output_filename}'.")


if __name__ == "__main__":
    # Create and run the event loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    write_data_to_csv()
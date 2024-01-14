import asyncio
import csv
import logging

import aiohttp
from bs4 import BeautifulSoup

from .custom_logger.logger import CustomFormatter

ColorfulHandler = logging.StreamHandler()
ColorfulHandler.setFormatter(CustomFormatter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        ColorfulHandler
    ]
)
logger = logging.getLogger(__name__)

async def fetch_html(session, url, retries=2):
    for _ in range(retries):
        try:
            async with session.get(url, timeout=20) as response:
                if response.status == 200:
                    return await response.text()

                elif response.status == 401:
                    logger.warning(
                        f"Failed to fetch HTML from {url}. THEY ARE PROTECTING THEIR DATA - Status code: {response.status}")

                else:
                    logger.warning(f"Failed to fetch HTML from {url}. Status code: {response.status}")

        except aiohttp.ClientError as e:
            logger.error(f"Error fetching HTML from {url}: {e}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout error. Retrying... ({url})")
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching HTML: {e}")

        # Retry logic
        logger.warning(f"Retrying... ({url})")

    return None


async def parse_main_html(html_code):
    return await asyncio.to_thread(_parse_main_html, html_code)


def _parse_main_html(html_code):
    try:
        soup = BeautifulSoup(html_code, 'html.parser')
        # company_name = soup.find('span', id='_vendor_', style='display:none').text.strip()
        # Extract URLs with 'Master' or 'Slave' in the text
        urls = [a['href'] for a in soup.find_all('a', href=True, string=['Master', 'Slave'])]
        return urls
    except Exception as e:
        logger.error(f"Error parsing main HTML, it seems that there is no 'Master' and 'Slave' URLs.: {e}")
        return []


async def parse_html(html_code):
    return await asyncio.to_thread(_parse_html, html_code)


def _parse_html(html_code):
    try:
        soup = BeautifulSoup(html_code, 'html.parser')
        company_name_element = soup.find('span', id='_vendor_', style='display:none')
        table_rows = soup.find_all('tr')

        data_dict = {}
        data_dict[
            'company_name'] = company_name_element.text.strip() if company_name_element else 'No company name found'

        for row in table_rows:
            columns = row.find_all('td')[:2]
            if len(columns) == 2:
                key = columns[0].text.strip()
                value = columns[1].text.strip()
                data_dict[key] = value

        return data_dict
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        return {}


async def process_page(session, ip):
    url = f"http://{ip}"
    html_code = await fetch_html(session, url)

    if html_code:
        result_data = []
        master_slave_urls = await parse_main_html(html_code)

        if master_slave_urls:
            # IP has 'Master' and 'Slave' URLs, proceed as usual
            logger.info(f"Found Master and Slave URLs for {url}: {master_slave_urls}")
            # Fetch and process data for each URL
            for master_slave_url in master_slave_urls:
                # Create a new session for each URL fetch
                async with aiohttp.ClientSession() as inner_session:
                    full_url = master_slave_url
                    data = await fetch_and_process_data(inner_session, full_url)
                    if data:
                        data['url'] = master_slave_url
                        result_data.append(data)

        else:
            logger.info(f"No valid Master and Slave URLs for {url}")
            data = await fetch_and_process_data(session, url)
            if data:
                data['url'] = url
                result_data.append(data)

        return result_data

    return None


async def fetch_and_process_data(session, url):
    html_code = await fetch_html(session, url)

    if html_code:
        data_dict = await parse_html(html_code)

        if data_dict:
            logger.info(f"Collected Data for: {url}")
            return data_dict
        else:
            logger.warning(f"No valid data for {url}")
    return None


def write_csv(output_file_path, fieldnames, data):
    with open(output_file_path, 'w', newline='') as csv_file:
        fieldnames = ['url'] + ['company_name'] + [name for name in fieldnames if name not in ['url', 'company_name']]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


async def main(ip_addresses):
    async with aiohttp.ClientSession() as session:
        tasks = [process_page(session, ip) for ip in ip_addresses]
        result_data_lists = await asyncio.gather(*tasks)

    return result_data_lists


def run_scraper(ip_addresses):
    result_data_lists = asyncio.run(main(ip_addresses))
    # Flatten the result_data_lists
    result_data = [data for result_data_list in result_data_lists if result_data_list for data in result_data_list]

    if result_data:

        result_data.sort(key=lambda x: x.get('company_name', ''))
        # Dynamically retrieve field names from the data
        keys = list(set(key for data in result_data for key in data.keys()))
        output_file_path = 'results/scraped_data_ensto.csv'
        write_csv(output_file_path, keys, result_data)
        logger.info(f"Data has been saved to '{output_file_path}'.")
    else:
        logger.warning("No valid data to save.")


if __name__ == "__main__":
    try:
        run_scraper(ip_addresses)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

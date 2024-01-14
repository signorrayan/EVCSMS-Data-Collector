import asyncio
import logging

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from .custom_logger.logger import CustomFormatter

# List of IP addresses you want to scrape
ColorfulHandler = logging.StreamHandler()
ColorfulHandler.setFormatter(CustomFormatter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        ColorfulHandler
    ]
)
logger = logging.getLogger(__name__)


async def fetch(session, url, retries=2):
    for _ in range(retries):
        try:
            async with session.get(url, timeout=20) as response:
                return await response.text()
        except aiohttp.client_exceptions.ServerDisconnectedError:
            logger.warning("Server disconnected. Retrying...")
        except asyncio.exceptions.TimeoutError:
            logger.error(f"Timeout error. Retrying... ({url})")
    # logger.info(f"Failed after {retries} retries. ({url})")
    return None


def extract_data_from_table(soup, section_header):
    """
    Extracts the table data below the header provided by section_header.
    This assumes each section has a corresponding h3 header.
    """
    headers = []
    data_rows = []

    # Locate the section and corresponding table
    header_tag = soup.find('h3', string=section_header)
    if header_tag and header_tag.find_next_sibling('table'):
        table = header_tag.find_next_sibling('table')
        # Extract headers from 'thead'
        headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
        # Extract data rows from 'tbody'
        data_rows = [td.get_text(strip=True) for td in table.find('tbody').find_all('td')]

    return headers or [], data_rows or []


async def scrape_info(session, ip):
    url = f'http://{ip}'
    html_content = await fetch(session, url)

    if html_content is None:
        logger.warning(f"Failed to fetch data from {url}. Skipping...")
        return None

    soup = BeautifulSoup(html_content, 'html.parser')

    # Scraping section by section
    evse_info_headers, evse_info_data = extract_data_from_table(soup, 'EVSE Access-Point:')
    evse_info_headers = ['Access-Point ' + header for header in evse_info_headers]
    csms_info_headers, csms_info_data = extract_data_from_table(soup, 'CSMS Connection:')
    csms_info_headers = ['CSMS ' + header for header in csms_info_headers]
    conn_status_headers, conn_status_data = extract_data_from_table(soup, 'Connection Status:')
    conn_status_headers = ['Connection ' + header for header in conn_status_headers]
    ethernet_settings_headers, ethernet_settings_data = extract_data_from_table(soup, 'Ethernet Settings:')
    ethernet_settings_headers = ['Ethernet ' + header for header in ethernet_settings_headers]
    bracket_info_headers, bracket_info_data = extract_data_from_table(soup, 'Installation Bracket information:')
    bracket_info_headers = ['Installation ' + header for header in bracket_info_headers]

    # Finding the software version and admin URL
    software_version = soup.find(string=lambda x: x.startswith('Software version:')).split(': ')[1].strip()
    admin_url = soup.find('a', string='Administration')['href']

    # Compile all the data into a dictionary
    scraped_data = {
        **dict(zip(evse_info_headers, evse_info_data)),
        **dict(zip(csms_info_headers, csms_info_data)),
        **dict(zip(conn_status_headers, conn_status_data)),
        **dict(zip(ethernet_settings_headers, ethernet_settings_data)),
        **dict(zip(bracket_info_headers, bracket_info_data)),
        'Software version': software_version,
        'Administration URL': url + admin_url,
    }
    logger.info(f"Collected Data for: {ip}")

    return scraped_data


async def main(ip_addresses):
    async with aiohttp.ClientSession() as session:
        tasks = [scrape_info(session, ip) for ip in ip_addresses]
        results = await asyncio.gather(*tasks)
        filtered_results = [result for result in results if result is not None]
        return filtered_results


def run_scraper(ip_addresses):
    result_data = asyncio.run(main(ip_addresses))
    if result_data:
        df = pd.DataFrame(result_data)
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_')
        df.set_index('connection_ip_address', inplace=True)
        df.reset_index(inplace=True)

        output_file_path = 'results/scraped_data_garo.csv'
        df.to_csv(output_file_path, index=False)
        logger.info(f"Data has been saved to '{output_file_path}'.")
    else:
        logger.warning("No valid data to save.")


if __name__ == "__main__":
    try:
        run_scraper(ip_addresses)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

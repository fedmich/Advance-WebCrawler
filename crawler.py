"""
Author: Fedmich + ChatGPT
Date: 2024-02-03
Version: v1.4.015
Description: Advanced Web Crawler with features such as timeouts, multi-threading, organized storage of data in txt files, error code logging, comprehensive error handling, and configurable settings. The script starts from the specified 'urls.txt' file, extracts information from web pages, and saves results in an organized manner. Check out the readme.md for a detailed list of features and usage instructions.
License: MIT License
"""

import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import time
import threading
import csv
from datetime import datetime

def script_path():
    return os.path.dirname(os.path.realpath(__file__))

def clean_filename(title):
    # Remove invalid characters for Windows filenames
    invalid_chars = r'[\/:*?"<>|]'
    cleaned_title = re.sub(invalid_chars, '', title)
    return cleaned_title

def apply_title_transform(title, title_search, title_replace):
    # Apply search and replace transformation on the title
    return title.replace(title_search, title_replace)

def get_page_content_with_rule(url, hostname, body_class_rule):
    try:
        # Send an HTTP request to the URL with a timeout of 10 seconds
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            # Parse the HTML content
            soup = BeautifulSoup(response.content, 'html.parser')

            body_text = ""
            # Extract title
            title = soup.find('h1').get_text() if soup.find('h1') else 'No Title'

            if body_class_rule:
                # Extract body content using the specified rule
                body_content = soup.select(body_class_rule)
                
                if body_content:
                    # Extract the text from the selected elements
                    body_text = ' '.join([element.get_text() for element in body_content])
            
            if not body_text:
                # If the rule doesn't match, use the default extraction method
                body_text = extract_default_body_content(soup)

            # Trim and remove script tags and unsafe HTML data
            body_text = re.sub(r'<script.*?</script>', '', body_text, flags=re.DOTALL)
            body_text = re.sub(r'<.*?>', '', body_text)
            
            body_text = body_text.strip()

            # Extract og:image URL
            og_image_tag = soup.find('meta', property='og:image')
            og_image_url = og_image_tag['content'] if og_image_tag else None

            return title, body_text, og_image_url
        else:
            return None, None, None
    except requests.Timeout:
        print(f'Timeout error for: {url}')
        return None, None, None
    except requests.RequestException as e:
        print(f'Error for {url}: {str(e)}')
        return None, None, None

def save_image(image_url, filename):
    # Check if the image file already exists and is not empty
    if os.path.exists(os.path.join(script_path(), 'data', f'{filename}.png')) and os.path.getsize(os.path.join(script_path(), 'data', f'{filename}.png')) > 0:
        print(f'Image file already exists for {filename}. Skipping download.')
        return

    try:
        # Send an HTTP request to the image URL
        response = requests.get(image_url, stream=True)

        if response.status_code == 200:
            # Save the image as a PNG file in the data directory
            with open(os.path.join(script_path(), 'data', f'{filename}.png'), 'wb') as image_file:
                for chunk in response.iter_content(chunk_size=128):
                    image_file.write(chunk)

            # Save the image URL into FILENAME-image.txt
            with open(os.path.join(script_path(), 'data', f'{filename}-image.txt'), 'w', encoding='utf-8') as url_file:
                url_file.write(image_url)
    except requests.RequestException as e:
        print(f'Error saving image for {filename}: {str(e)}')

def save_url(url, filename):
    # Save the URL into FILENAME-url.txt
    with open(os.path.join(script_path(), 'data', f'{filename}-url.txt'), 'w', encoding='utf-8') as url_file:
        url_file.write(url)

def get_delay(hostname):
    # Read delay from configuration file
    config_file = os.path.join(script_path(), 'config', f'{hostname}.txt')
    if os.path.exists(config_file):
        with open(config_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if line.startswith('delay='):
                    return int(line.split('=')[1])
    return 0  # Default delay is 0 seconds

def get_body_class(hostname):
    # Read body class rule from configuration file
    config_file = os.path.join(script_path(), 'config', f'{hostname}.txt')
    if os.path.exists(config_file):
        with open(config_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if line.startswith('body_class='):
                    return line.split('=')[1].strip()
    return ''  # Default body class rule is an empty string


def log_successful_crawl(url, log_directory):
    # Create directory if it doesn't exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Log successful crawls to successes.csv
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_data = [url, '200', timestamp]

    log_file_path = os.path.join(log_directory, 'successes.csv')
    with open(log_file_path, 'a', newline='', encoding='utf-8') as logs_csv:
        csv_writer = csv.writer(logs_csv, delimiter=',')
        csv_writer.writerow(log_data)

def log_failed_url(url, log_directory):
    # Create directory if it doesn't exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Log failed URLs to fails.txt
    with open(os.path.join(log_directory, 'fails.txt'), 'a', encoding='utf-8') as fails_file:
        fails_file.write(f'{url}\n')

def log_to_csv(url, status_code, log_directory):
    # Create directory if it doesn't exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Log URL, status code, and timestamp to logs.csv
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_data = [url, str(status_code), timestamp]

    log_file_path = os.path.join(log_directory, 'logs.csv')
    with open(log_file_path, 'a', newline='', encoding='utf-8') as logs_csv:
        csv_writer = csv.writer(logs_csv, delimiter=',')
        csv_writer.writerow(log_data)

def crawl_page_with_rule(url, log_directory, title_search, title_replace, body_class_rule):
    title, body, og_image_url = get_page_content_with_rule(url, title_search, body_class_rule)

    if title and body:
        # Apply title transformation from configuration
        title = apply_title_transform(title, title_search, title_replace)
        title = title.strip()

        # Create a clean filename for the text file
        filename = clean_filename(title)

        # Write title and body to the text file
        with open(os.path.join(script_path(), 'data', f'{filename}.txt'), 'w', encoding='utf-8') as output_file:
            # Format the txt file
            output_file.write(f'{title}\n---\n{body}\n')

        # Save the URL in a separate FILENAME-url.txt
        save_url(url, filename)

        print(f'Successfully crawled: {url} -> data/{filename}.txt')

        if og_image_url:
            # Save the og:image as a PNG file in the data directory
            save_image(urljoin(url, og_image_url), filename)
            print(f'Saved og:image: {urljoin(url, og_image_url)} -> data/{filename}.png')
        else:
            print(f'No og:image found for: {url}')
        
         # Log successful crawl
        log_successful_crawl(url, log_directory)
        
    else:
        print(f'Failed to crawl: {url}')
        log_failed_url(url, log_directory)

def crawl_websites(urls, max_threads, title_search, title_replace):
    # Create a list to store thread objects
    threads = []

    # Use a Semaphore to limit the number of concurrent threads
    semaphore = threading.Semaphore(max_threads)

    # Get current date for log directory (Y-m-d format)
    ymd = datetime.now().strftime('%Y-%m-%d')
    log_directory = os.path.join(script_path(), 'results', ymd)

    # Create the 'data' subdirectory if it doesn't exist
    data_directory = os.path.join(script_path(), 'data')
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)

    for url in urls:
        # Extract hostname from the URL
        hostname = requests.utils.urlparse(url).hostname

        # Get delay from configuration
        delay = get_delay(hostname)
        body_class_rule = get_body_class(hostname)

        # Acquire the semaphore to limit concurrent threads
        semaphore.acquire()

        # Start a new thread for crawling the page
        thread = threading.Thread(target=crawl_page_with_rule, args=(url, log_directory, title_search, title_replace, body_class_rule ))
        thread.start()
        threads.append(thread)

        # Release the semaphore after the delay
        time.sleep(delay)
        semaphore.release()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


def extract_default_body_content(soup):
    # Extract body content using meta content or the first paragraph
    meta_description = soup.find('meta', {'name': 'description'})
    body_content = meta_description['content'] if meta_description else None

    if not body_content:
        first_paragraph = soup.find('p')
        body_content = first_paragraph.get_text() if first_paragraph else 'No Body'

    return body_content


if __name__ == '__main__':
    # Read global configuration for maximum threads
    global_config_file = os.path.join(script_path(), 'config', 'global-engine-config.txt')
    max_threads = 4  # Default value if the config file is not found
    title_search = ''  # Default value for title search pattern
    title_search = ''  # Default value for title search pattern
    
    if os.path.exists(global_config_file):
        with open(global_config_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if line.startswith('max_threads='):
                    max_threads = int(line.split('=')[1])
                elif line.startswith('title_search='):
                    title_search = line.split('=')[1].strip()
                elif line.startswith('title_replace='):
                    title_replace = line.split('=')[1].strip()
                
    input_file = os.path.join(script_path(), 'urls.txt')
    with open(input_file, 'r') as file:
        urls = [url.strip() for url in file.read().splitlines() if url.strip()]


    crawl_websites(urls, max_threads, title_search, title_replace)

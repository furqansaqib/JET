import json
import ast
import yaml
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
from pathlib import Path
import requests
import os
import threading
import gzip
import io

#Read context variables from Yaml File
def read_context_variables():
    global server, database, username, password, port, email_to, email_from, product_url, reviews_url, product_dest, reviews_dest, dest_folder, product_unzipped, reviews_unzipped
    try:
        with open("D:\Interviews\JET\code\context_file.yml", 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: Context file not found.")
        return False
    except yaml.YAMLError as e:
        print("Error: Failed to load context file.")
        print(e)
        return False

    try:
        #Database Credentials
        server=config['psql']['server']
        database=config['psql']['database']
        username=config['psql']['username']
        password=config['psql']['password']
        port=config['psql']['port']
        #email variables
        email_to=config['email']['to']
        email_from=config['email']['cc']
        #Source files Url (Download From)
        product_url=config['source']['product']
        reviews_url=config['source']['reviews']
        #Destination Paths (Download to)
        product_dest=config['destination']['product']
        reviews_dest=config['destination']['reviews']
        dest_folder=config['destination']['reviews']
        product_unzipped=config['unzipped']['product']
        reviews_unzipped=config['unzipped']['reviews']
    except KeyError as e:
        print(f"Error: Missing key in context file: {e}")
        return False
    
    print("Context Variables Loaded")
    return True

def fix_file_extension(directory):
    old_extension = ".gz2"
    new_extension = ".json"

    # loop through all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(old_extension):
        # construct the new file name with the updated extension
            new_filename = os.path.splitext(filename)[0] + new_extension
        
            # rename the file
            os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))

def download_filechunk(url, file_path, start_byte, end_byte):
    try:
        # send HTTP GET request to download the file
        response = requests.get(url, headers={'Range': f'bytes={start_byte}-{end_byte-1}'}, stream=True)
        response.raise_for_status()
        # open file for writing in binary mode
        with open(file_path, 'r+b') as file:
            # move file pointer to the starting position of the chunk
            file.seek(start_byte)
            # write the file contents in chunks to improve performance
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except (requests.exceptions.RequestException, IOError) as e:
        print(f'Error occurred while downloading file chunk: {e}')

def download_file_in_threads(url, destination_folder, chunk_size):
    try:
        file_name = os.path.basename(url)
        # create destination folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # get file size to calculate number of chunks
        response = requests.head(url)
        response.raise_for_status()
        file_size = int(response.headers.get('content-length', 0))

        # calculate chunk size and number of threads
        chunk_size = 1024 * 1024 * 1024 * chunk_size
        num_threads = file_size // chunk_size + 1

        # create file with zeros to ensure that file size matches content length
        file_path = os.path.join(destination_folder, file_name)
        with open(file_path, 'w+b') as file:
            file.write(b'\0' * file_size)

        # create threads to download file in chunks
        threads = []
        for i in range(num_threads):
            start_byte = i * chunk_size
            end_byte = min((i + 1) * chunk_size, file_size)
            thread = threading.Thread(target=download_filechunk, args=(url, file_path, start_byte, end_byte))
            thread.start()
            threads.append(thread)

        # wait for all threads to finish
        for thread in threads:
            thread.join()
        print(f'{file_name} downloaded to {destination_folder}')
        fix_file_extension(destination_folder)
    except (requests.exceptions.RequestException, IOError) as e:
        print(f'Error occurred while downloading file: {e}')



def uncompress_file(gz_path, dest_folder, chunk_size=1024*1024*1024*4):
    try:
        # Create the destination folder if it doesn't exist
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)

        # Construct the destination file path
        file_name = "metadata.json.gz"
        dest_path = os.path.join(dest_folder, file_name[:-3])

        # Open the gzipped file and read its contents in chunks
        with gzip.GzipFile(gz_path, 'rb') as gz_file, open(dest_path, 'w') as json_file:
            buffer = io.BufferedReader(gz_file, chunk_size)
            while True:
                chunk = buffer.read(chunk_size)
                if not chunk:
                    break
                decoded_chunk = chunk.decode('utf-8')
                json_file.write(decoded_chunk)

        print(f'{file_name} extracted to {dest_path}')
    except Exception as e:
        print(f'Error occurred while uncompressing file: {e}')

#Create Database Connection
def create_pg_connection(server,database,username,password,port):
    try:
        global conn
        conn = psycopg2.connect(host=server, dbname=database, user=username, password=password, port=port)
        conn_string = 'postgresql+psycopg2://'+username+':'+password+'@'+server+':'+str(port)+'/'+database
        engine = create_engine(conn_string)
        print('Connection created')
    except psycopg2.Error as e:
        print(f'Error creating database connection: {e}')

#Function to load products data to database line by line
def load_products_dataset(file_path):
    try:
        with open(file_path, "r") as f:
            for line in f:
                # Parse the JSON object
                json_dat = json.dumps(ast.literal_eval(line))
                data = json.loads(json_dat)

                # Get the values for each key
                asin = data.get("asin", None)
                sales_rank = data.get("salesRank", None)
                categories = data.get("categories", None)
                also_viewed = data.get("related", {}).get("also_viewed", None)
                also_bought = data.get("related", {}).get("also_bought", None)
                bought_together = data.get("related", {}).get("bought_together", None)
                buy_after_viewing = data.get("related", {}).get("buy_after_viewing", None)
                im_url = data.get("imUrl", None)
                description = data.get("description", None)
                title = data.get("title", None)
                price = data.get("price", None)

                if sales_rank is not None:
                    sales_rank = str(sales_rank)
                if categories is not None:
                    categories = str(categories)

                # Insert the data into the PostgreSQL table
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO justeattakeaway.products_stg (asin, title, im_url, price, sales_rank, categories, also_viewed, also_bought, bought_together, buy_after_viewing, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (asin, title, im_url, price, sales_rank, categories, also_viewed, also_bought, bought_together, buy_after_viewing, description)
                    )
                conn.commit()
    except FileNotFoundError as e:
        print(f"Error: File not found: {e.filename}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e.msg}")
    except Exception as e:
        print(f"Error: {e}")
    
#Function to load reviews data to database line by line
def load_reviews_dataset(file_path):
    try:
        counter = 0
        # Open the JSON file and read each line
        with open(file_path, "r") as f:
            for line in f:
                counter += 1
                if counter > 55682627:
                    # Remove NULL Bytes from string
                    line = line.replace("\\x00", "")
                    line = line.replace("\\u0000", "")

                    # Parse the JSON object
                    data = json.loads(line)

                    # Get the values for each key, or use "NA" if not available
                    reviewerID = str(data.get("reviewerID", None))
                    asin = data.get("asin", None)
                    reviewerName = str(data.get("reviewerName", None))
                    helpful = str(data.get("helpful", "NA"))
                    reviewText = str(data.get("reviewText", None))
                    overall = str(data.get("overall", "NA"))
                    summary = data.get("summary", None)
                    unix_time = data.get("unixReviewTime", None)
                    review_date = (
                        datetime.fromtimestamp(unix_time).date() if unix_time else None
                    )

                    # Insert the data into the PostgreSQL table
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO jet.reviews_stg (reviewerID, asin, reviewerName, helpful, overall,summary, review_date, reviewText) VALUES (%s, %s, %s, %s,%s, %s, %s, %s)",
                            (
                                reviewerID,
                                asin,
                                reviewerName,
                                helpful,
                                overall,
                                summary,
                                review_date,
                                reviewText,
                            ),
                        )
                    conn.commit()
                else:
                    continue
    except (IOError, OSError) as e:
        print(f"Error loading file: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()  # Close the database connection

if __name__ == "__main__":
    read_context_variables()
    download_file_in_threads(product_url,dest_folder,4) # data fetched by each thread = 4GB
    download_file_in_threads(reviews_url,dest_folder,4) # data fetched by each thread = 4GB
    uncompress_file(product_dest,dest_folder) #uncompress product Metadata File
    uncompress_file(reviews_dest,dest_folder) #uncompress review data
    create_pg_connection(server,database,username,password,port) #Create Database Connection
    load_products_dataset(product_unzipped) #load product data into PSQL
    load_reviews_dataset(reviews_unzipped) #loads review data into PSQL

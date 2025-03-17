import os
from requests import get, post
from shutil import move
from dotenv import load_dotenv
from os import getenv
from typing import Dict, Any
from download import download
import requests
load_dotenv()  # Load environment variables from .env file

def get_token() -> str:
    """
    Get the access token for GoFile API.

    :return: The access token.
    """
    user_agent: str | None = getenv("GF_USERAGENT")
    headers: dict[str, str] = {
        "User-Agent": user_agent if user_agent else "Mozilla/5.0",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
        "Connection": "keep-alive",
    }

    create_account_response: dict = post("https://api.gofile.io/accounts", headers=headers).json()

    if create_account_response["status"] != "ok":
        raise Exception("Account creation failed!")

    return create_account_response["data"]["token"]

def download_file(download_path: str, url: str) -> str:
    """
    Download a file from a GoFile link or a folder to the specified download path.

    :param download_path: Directory where the file/folder will be saved.
    :param url: URL of the file/folder to download.
    :return: Full file/folder path where the content is saved.
    """
    # Ensure the download path exists
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Get the GoFile API token
    token = get_token()

    # Get the content ID from the URL
    content_id = url.split("/")[-1]

    # Construct the API URL to check if the link is a file or folder
    api_url = f"https://api.gofile.io/contents/{content_id}?wt=4fd6sg89d7s6&cache=true"
    headers = {
        "Authorization": f"Bearer {token}",
    }

    response = get(api_url, headers=headers).json()

    if response["status"] != "ok":
        raise Exception("Failed to get content information from GoFile.")

    # Get file/folder data from the response
    data = response["data"]
    
    if data["type"] == "file":
        # If it's a file, download it
        return download_individual_file(download_path, data)
    elif data["type"] == "folder":
        # If it's a folder, download the whole folder recursively
        children = data["children"]
        return download_individual_file(download_path=download_path,data=children[next(iter(children))]) # put first file
    else:
        raise Exception("Invalid content type. Expected file or folder.")

def download_individual_file(download_path: str, data: Dict[str, Any]) -> str:
    """
    Download an individual file from GoFile.

    :param download_path: Directory where the file will be saved.
    :param data: File data from the GoFile API.
    :return: Full file path where the file is saved.
    """
    filename = data["name"]

    # Set up the file path
    file_path = os.path.join(download_path, filename)
    x = str(data["link"])
    try:
        # Send a GET request to download the file
        response = requests.get(x, stream=True)

        # Check if the request was successful
        if response.status_code == 200:
            # Open the file in write-binary mode and save the content
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            print(f"File downloaded successfully to: {file_path}")
            return file_path
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error occurred: {e}")
        return None
    

if __name__ == "__main__":
    download_path = 'books_files'  # Example folder
    url = 'https://gofile.io/d/brifl1'  # Example URL from GoFile

    final_path = download_file(download_path, url)
    print(f"Downloaded content to: {final_path}")

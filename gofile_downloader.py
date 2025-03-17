#! /usr/bin/env python3


from os import chdir, getcwd, getenv, listdir, mkdir, path, rmdir
from sys import exit, stdout, stderr
from typing import Any, NoReturn, TextIO
from requests import get, post
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from platform import system
from hashlib import sha256
from shutil import move
from time import perf_counter


NEW_LINE: str = "\n" if system() != "Windows" else "\r\n"

class Download:
    def __init__(self, url: str, password: str | None = None, max_workers: int = 5, download_path: str | None = None) -> None:
        root_dir: str | None = getenv("GF_DOWNLOADDIR")

        if root_dir and path.exists(root_dir):
            chdir(root_dir)

        self._lock: Lock = Lock()
        self._max_workers: int = max_workers
        token: str | None = getenv("GF_TOKEN")
        self._message: str = " "
        self._content_dir: str | None = None
        self._download_path: str = download_path if download_path else getcwd()  # Default to current working directory

        # Dictionary to hold information about file and its directories structure
        # {"index": {"path": "", "filename": "", "link": ""}}
        # where the largest index is the top most file
        self._files_info: dict[str, dict[str, str]] = {}

        self._root_dir: str = root_dir if root_dir else getcwd()
        self._token: str = token if token else self._get_token()

        self._parse_url_or_file(url, password)


    @staticmethod
    def _get_token() -> str:
        """
        _get_token

        Gets the access token of account created.

        :return: The access token of an account. Or exit if account creation fail.
        """

        user_agent: str | None = getenv("GF_USERAGENT")
        headers: dict[str, str] = {
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }

        create_account_response: dict[Any, Any] = post("https://api.gofile.io/accounts", headers=headers).json()

        if create_account_response["status"] != "ok":
            die("Account creation failed!")

        return create_account_response["data"]["token"]


    def _download_content(self, file_info: dict[str, str], chunk_size: int = 16384) -> None:
        """
        _download_content

        Requests the contents of the file and writes it.

        :param file_info: a dictionary with information about a file to be downloaded.
        :param chunk_size: the number of bytes it should read into memory.
        :return:
        """

        filepath: str = path.join(file_info["path"], file_info["filename"])
        if path.exists(filepath):
            if path.getsize(filepath) > 0:
                _print(f"{filepath} already exist, skipping.{NEW_LINE}")

                return

        tmp_file: str =  f"{filepath}.part"
        url: str = file_info["link"]
        user_agent: str | None = getenv("GF_USERAGENT")

        headers: dict[str, str] = {
            "Cookie": f"accountToken={self._token}",
            "Accept-Encoding": "gzip, deflate, br",
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept": "*/*",
            "Referer": f"{url}{('/' if not url.endswith('/') else '')}",
            "Origin": url,
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache"
        }

        # check for partial download and resume from last byte
        part_size: int = 0
        if path.isfile(tmp_file):
            part_size = int(path.getsize(tmp_file))
            headers["Range"] = f"bytes={part_size}-"

        has_size: str | None = None
        status_code: int | None = None

        try:
            with get(url, headers=headers, stream=True, timeout=(9, 27)) as response_handler:
                status_code = response_handler.status_code

                if ((response_handler.status_code in (403, 404, 405, 500)) or
                    (part_size == 0 and response_handler.status_code != 200) or
                    (part_size > 0 and response_handler.status_code != 206)):
                    _print(
                        f"Couldn't download the file from {url}."
                        f"{NEW_LINE}"
                        f"Status code: {status_code}"
                        f"{NEW_LINE}"
                    )

                    return

                content_length: str | None = response_handler.headers.get("Content-Length")
                content_range: str | None = response_handler.headers.get("Content-Range")
                has_size = content_length if part_size == 0 \
                    else content_range.split("/")[-1] if content_range else None

                if not has_size:
                    _print(
                        f"Couldn't find the file size from {url}."
                        f"{NEW_LINE}"
                        f"Status code: {status_code}"
                        f"{NEW_LINE}"
                    )

                    return

                with open(tmp_file, "ab") as handler:
                    total_size: float = float(has_size)

                    start_time: float = perf_counter()
                    for i, chunk in enumerate(response_handler.iter_content(chunk_size=chunk_size)):
                        progress: float = (part_size + (i * len(chunk))) / total_size * 100

                        handler.write(chunk)

                        rate: float = (i * len(chunk)) / (perf_counter()-start_time)
                        unit: str = "B/s"
                        if rate < (1024):
                            unit = "B/s"
                        elif rate < (1024*1024):
                            rate /= 1024
                            unit = "KB/s"
                        elif rate < (1024*1024*1024):
                            rate /= (1024 * 1024)
                            unit = "MB/s"
                        elif rate < (1024*1024*1024*1024):
                            rate /= (1024 * 1024 * 1024)
                            unit = "GB/s"

                        # thread safe update the self._message, so no output interleaves
                        with self._lock:
                            _print(f"\r{' ' * len(self._message)}")

                            self._message = f"\rDownloading {file_info['filename']}: {part_size + i * len(chunk)}" \
                            f" of {has_size} {round(progress, 1)}% {round(rate, 1)}{unit}"

                            _print(self._message)
        finally:
            with self._lock:
                if has_size and path.getsize(tmp_file) == int(has_size):
                    _print(f"\r{' ' * len(self._message)}")
                    _print(f"\rDownloading {file_info['filename']}: "
                        f"{path.getsize(tmp_file)} of {has_size} Done!"
                        f"{NEW_LINE}"
                    )
                    move(tmp_file, filepath)


    def _parse_links_recursively(
        self,
        content_id: str,
        password: str | None = None,
        pathing_count: dict[str, int] = {},
        recursive_files_index: dict[str, int] = {"index": 0}
    ) -> None:
        """
        _parse_links_recursively

        Parses for possible links recursively and populate a list with file's info
        while also creating directories and subdirectories.

        :param content_id: url to the content.
        :param password: content's password.
        :param pathing_count: pointer-like object for keeping track of naming collision of pathing (filepaths and
                              directories) should only be internally used by this function to keep object state track.
        :param recursive_files_index: pointer-like object for keeping track of files indeces,
                                      should only be internally used by this function toakeep object state track.
        :return:
        """

        url: str = f"https://api.gofile.io/contents/{content_id}?wt=4fd6sg89d7s6&cache=true&sortField=createTime&sortDirection=1"

        if password:
            url = f"{url}&password={password}"

        user_agent: str | None = getenv("GF_USERAGENT")

        headers: dict[str, str] = {
            "User-Agent": user_agent if user_agent else "Mozilla/5.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "Authorization": f"Bearer {self._token}",
        }

        response: dict[Any, Any] = get(url, headers=headers).json()

        if response["status"] != "ok":
            _print(f"Failed to get a link as response from the {url}.{NEW_LINE}")
            return

        data: dict[Any, Any] = response["data"]

        if "password" in data and "passwordStatus" in data and data["passwordStatus"] != "passwordOk":
            _print(f"Password protected link. Please provide the password.{NEW_LINE}")
            return

        if data["type"] != "folder":
            current_dir: str = self._download_path
            filename: str = data["name"]
            recursive_files_index["index"] += 1
            filepath: str = path.join(current_dir, filename)

            if filepath in pathing_count:
                pathing_count[filepath] += 1
            else:
                pathing_count[filepath] = 0

            if pathing_count and pathing_count[filepath] > 0:
                extension: str
                filename, extension = path.splitext(filename)
                filename = f"{filename}({pathing_count[filepath]}){extension}"

            self._files_info[str(recursive_files_index["index"])] = {
                "path": current_dir,
                "filename": filename,
                "link": data["link"]
            }

            return

        folder_name: str = data["name"]

        if not self._content_dir and folder_name != content_id:
            self._content_dir = path.join(self._download_path, content_id)

            self._create_dir(self._content_dir)
            chdir(self._content_dir)
        elif not self._content_dir and folder_name == content_id:
            self._content_dir = path.join(self._download_path, content_id)
            self._create_dir(self._content_dir)

        absolute_path: str = path.join(getcwd(), folder_name)

        if absolute_path in pathing_count:
            pathing_count[absolute_path] += 1
        else:
            pathing_count[absolute_path] = 0

        if pathing_count and pathing_count[absolute_path] > 0:
            absolute_path = f"{absolute_path} ({pathing_count[absolute_path]})"

        self._create_dir(absolute_path)

        if pathing_count and pathing_count[absolute_path] > 0:
            pathing_count[absolute_path] += 1

        recursive_files_index["index"] += 1

        if "content" in data:
            for content in data["content"]:
                self._parse_links_recursively(content["contentId"], password, pathing_count, recursive_files_index)


    def _parse_url_or_file(self, url: str, password: str | None = None) -> None:
        """
        _parse_url_or_file

        Parse URL or file path, to determine whether we want to download or parse.

        :param url: URL or file path to use.
        :param password: optional password for protected contents.
        :return:
        """

        if not url.startswith("https://gofile.io/"):
            die("You need to provide a valid link to a file or folder hosted on GoFile!")

        content_id: str = url.split("/")[-1]

        self._parse_links_recursively(content_id, password)


    def start(self) -> str:
        """
        start

        The entry point, process the download task.

        :return: The final download directory where files were saved.
        """
        self._threaded_downloads()
        return self._content_dir


# Example Usage
if __name__ == "__main__":
    url = 'https://gofile.io/d/nT4m1t'  # Example URL from GoFile
    download_path = '/path/to/your/desired/directory'

    main_task = Download(url, max_workers=4, download_path=download_path)
    final_directory = main_task.start()
    print(f"Downloaded files are saved in {final_directory}")

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from google.cloud import storage
import io
import zipfile
import logging
from aiohttp import ClientTimeout
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_ATTEMPTS = 10
TIMEOUT = 300


class AsyncMortgageDataScraper:
    def __init__(self, username, password, data_source, gcs_bucket_name):
        self.username = username
        self.password = password
        self.data_source = data_source
        self.gcs_bucket_name = gcs_bucket_name
        self.session = None
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.gcs_bucket_name)

    async def login(self):
        """Modify as needed in partner specific class"""
        pass

    async def get_download_links(self):
        """Modify as needed in partner specific class"""
        pass

    async def extract_nested_zip(self, zip_content):
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            for filename in zf.namelist():
                if filename.endswith(".zip"):
                    with zf.open(filename) as nested_zip:
                        nested_content = nested_zip.read()
                        async for (
                            nested_filename,
                            nested_file_content,
                        ) in self.extract_nested_zip(nested_content):
                            yield nested_filename, nested_file_content
                else:
                    with zf.open(filename) as file:
                        yield filename, file.read()

    def file_exists_in_gcs(self, gcs_path):
        blob = self.bucket.blob(gcs_path)
        return blob.exists()

    @retry(
        stop=stop_after_attempt(DEFAULT_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(
            (aiohttp.ClientPayloadError, aiohttp.ClientError, asyncio.TimeoutError)
        ),
    )
    async def download_file(self, link):
        timeout = ClientTimeout(total=TIMEOUT)
        async with self.session.get(link, timeout=timeout) as response:
            if response.status == 200:
                return await response.read()
            else:
                raise Exception(
                    f"Failed to download {link}, status code: {response.status}"
                )

    async def download_and_upload_to_gcs(self, link):
        try:
            filename = link.split("/")[-1]
            gcs_path = f"{self.data_source}/raw/{filename}"

            if self.file_exists_in_gcs(gcs_path):
                logger.info(f"File {filename} already exists in GCS. Skipping.")
                return [gcs_path]

            content = await self.download_file(link)
            uploaded_files = []

            async for nested_filename, file_content in self.extract_nested_zip(content):
                nested_gcs_path = f"{self.data_source}/raw/{nested_filename}"
                if not self.file_exists_in_gcs(nested_gcs_path):
                    blob = self.bucket.blob(nested_gcs_path)
                    blob.upload_from_string(file_content)
                    uploaded_files.append(nested_gcs_path)
                    logger.info(f"Uploaded {nested_filename} to GCS")
                else:
                    logger.info(
                        f"File {nested_filename} already exists in GCS. Skipping."
                    )
                    uploaded_files.append(nested_gcs_path)

            return uploaded_files
        except Exception as e:
            logger.error(f"Error processing {link}: {str(e)}")
            return []

    async def scrape(self):
        async with aiohttp.ClientSession() as self.session:
            await self.login()
            links = await self.get_download_links()
            tasks = [self.download_and_upload_to_gcs(link) for link in links]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_uploads = [
                item
                for sublist in results
                if isinstance(sublist, list)
                for item in sublist
            ]
            return successful_uploads


class FreddieMacScraper(AsyncMortgageDataScraper):
    BASE_URL = "https://freddiemac.embs.com/FLoan"
    AUTH_URL = f"{BASE_URL}/secure/auth.php"
    DOWNLOAD_URL = f"{BASE_URL}/Data/download.php"

    @retry(
        stop=stop_after_attempt(DEFAULT_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=4, max=TIMEOUT),
        retry=retry_if_exception_type(
            (aiohttp.ClientPayloadError, aiohttp.ClientError, asyncio.TimeoutError)
        ),
    )
    async def login(self):
        payload = {"username": self.username, "password": self.password}
        async with self.session.post(self.AUTH_URL, data=payload) as response:
            if response.status != 404:
                raise Exception("Login failed")

        payload2 = {
            "accept": "Yes",
            "acceptSubmit": "Continue",
            "action": "acceptTandC",
        }
        async with self.session.post(self.DOWNLOAD_URL, data=payload2) as response:
            if response.status != 200:
                raise Exception("Accepting terms and conditions failed")

    async def get_download_links(self):
        async with self.session.get(self.DOWNLOAD_URL) as response:
            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")
            return [
                f"{self.BASE_URL}/Data/{a['href']}"
                for a in soup.find_all("a", href=True)
                if "data_time" in a["href"]
            ]

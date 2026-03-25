# data_engineering/ingestion/base_client.py

# data_engineering/ingestion/base_client.py
from curl_cffi import requests
import logging
from typing import Optional, Dict, Any
import os

class StealthClient:
    """
    A fortified HTTP client that mimics modern browser TLS fingerprints (JA3/JA4) 
    to bypass WAFs and anti-bot protections on target governmental portals.
    """
    def __init__(self, proxy_url: Optional[str] = None):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        # Utilize rotating residential proxies to bypass geographic rate limiting
        self.proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        
        # Impersonate a modern Chrome browser to generate a legitimate TLS fingerprint
        self.session = requests.Session(impersonate="chrome120", proxies=self.proxies)
        self.session.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        })

    def fetch_get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        try:
            self.logger.info(f"Fetching: {url}")
            response = self.session.get(url, params=params, timeout=45)
            response.raise_for_status()
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}. Potential WAF Block or Timeout. Error: {e}")
            raise
            
    def download_file(self, url: str, save_path: str):
        """Streams large binary payloads (PDFs, ZIPs) to disk."""
        self.logger.info(f"Initiating download from {url} to {save_path}")
        response = self.session.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        self.logger.info(f"Successfully downloaded artifact to {save_path}")
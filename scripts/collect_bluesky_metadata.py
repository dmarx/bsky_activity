# scripts/collect_bluesky_metadata.py
"""
Bluesky metadata collection script for fraud detection analysis.
Collects public profile information for specified users.
"""

import asyncio
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import fire
from loguru import logger
from omegaconf import DictConfig, OmegaConf


class BlueskyMetadataCollector:
    """Collects public metadata from Bluesky profiles for fraud detection."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = OmegaConf.load(config_path)
        self.session: Optional[aiohttp.ClientSession] = None
        self.base_url = "https://bsky.social/xrpc"
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "BlueskyMetadataCollector/1.0"}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def get_profile_metadata(self, handle: str) -> Dict:
        """
        Collect public metadata for a single Bluesky profile.
        
        Args:
            handle: Bluesky handle (e.g., 'user.bsky.social')
            
        Returns:
            Dictionary containing profile metadata
        """
        try:
            logger.info(f"Collecting metadata for: {handle}")
            
            # Get profile information
            profile_url = f"{self.base_url}/com.atproto.repo.describeRepo"
            params = {"repo": handle}
            
            async with self.session.get(profile_url, params=params) as response:
                if response.status != 200:
                    logger.warning(f"Failed to get repo info for {handle}: {response.status}")
                    return self._create_error_record(handle, f"HTTP {response.status}")
                
                repo_data = await response.json()
            
            # Get detailed profile
            profile_detail_url = f"{self.base_url}/app.bsky.actor.getProfile"
            profile_params = {"actor": handle}
            
            async with self.session.get(profile_detail_url, params=profile_params) as response:
                if response.status != 200:
                    logger.warning(f"Failed to get profile for {handle}: {response.status}")
                    return self._create_error_record(handle, f"Profile HTTP {response.status}")
                
                profile_data = await response.json()
            
            # Extract relevant metadata
            metadata = {
                "handle": handle,
                "did": repo_data.get("did", ""),
                "display_name": profile_data.get("displayName", ""),
                "description": profile_data.get("description", ""),
                "followers_count": profile_data.get("followersCount", 0),
                "follows_count": profile_data.get("followsCount", 0),
                "posts_count": profile_data.get("postsCount", 0),
                "created_at": repo_data.get("createdAt", ""),
                "indexed_at": profile_data.get("indexedAt", ""),
                "avatar_url": profile_data.get("avatar", ""),
                "banner_url": profile_data.get("banner", ""),
                "verified": profile_data.get("verified", False),
                "collection_timestamp": datetime.now(timezone.utc).isoformat(),
                "collection_status": "success"
            }
            
            logger.info(f"Successfully collected metadata for {handle}")
            return metadata
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout collecting metadata for {handle}")
            return self._create_error_record(handle, "timeout")
        except Exception as e:
            logger.error(f"Error collecting metadata for {handle}: {str(e)}")
            return self._create_error_record(handle, str(e))
    
    def _create_error_record(self, handle: str, error: str) -> Dict:
        """Create an error record for failed collections."""
        return {
            "handle": handle,
            "did": "",
            "display_name": "",
            "description": "",
            "followers_count": 0,
            "follows_count": 0,
            "posts_count": 0,
            "created_at": "",
            "indexed_at": "",
            "avatar_url": "",
            "banner_url": "",
            "verified": False,
            "collection_timestamp": datetime.now(timezone.utc).isoformat(),
            "collection_status": f"error: {error}"
        }
    
    async def collect_batch_metadata(self, handles: List[str]) -> List[Dict]:
        """
        Collect metadata for a batch of handles with rate limiting.
        
        Args:
            handles: List of Bluesky handles
            
        Returns:
            List of metadata dictionaries
        """
        semaphore = asyncio.Semaphore(self.config.rate_limit.max_concurrent)
        delay = self.config.rate_limit.delay_seconds
        
        async def collect_with_rate_limit(handle: str) -> Dict:
            async with semaphore:
                result = await self.get_profile_metadata(handle)
                await asyncio.sleep(delay)
                return result
        
        tasks = [collect_with_rate_limit(handle) for handle in handles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception for handle {handles[i]}: {result}")
                processed_results.append(self._create_error_record(handles[i], str(result)))
            else:
                processed_results.append(result)
        
        return processed_results
    
    def load_usernames(self, input_file: str) -> List[str]:
        """Load usernames from input file (supports .txt, .csv, .json)."""
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        if input_path.suffix == '.txt':
            return input_path.read_text().strip().split('\n')
        elif input_path.suffix == '.csv':
            with open(input_path, 'r') as f:
                reader = csv.reader(f)
                # Assume first column contains usernames
                return [row[0] for row in reader if row]
        elif input_path.suffix == '.json':
            with open(input_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'usernames' in data:
                    return data['usernames']
                else:
                    raise ValueError("JSON file must contain a list or dict with 'usernames' key")
        else:
            raise ValueError(f"Unsupported file format: {input_path.suffix}")
    
    def save_results(self, results: List[Dict], output_file: str):
        """Save results to output file."""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if output_path.suffix == '.csv':
            with open(output_path, 'w', newline='') as f:
                if results:
                    writer = csv.DictWriter(f, fieldnames=results[0].keys())
                    writer.writeheader()
                    writer.writerows(results)
        elif output_path.suffix == '.json':
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
        else:
            raise ValueError(f"Unsupported output format: {output_path.suffix}")
        
        logger.info(f"Results saved to {output_path}")
    
    async def run_collection(
        self,
        input_file: str = "data/usernames.txt",
        output_file: str = "data/metadata.csv"
    ):
        """
        Main collection workflow.
        
        Args:
            input_file: Path to file containing usernames
            output_file: Path for output file
        """
        logger.info("Starting Bluesky metadata collection")
        
        # Load usernames
        usernames = self.load_usernames(input_file)
        logger.info(f"Loaded {len(usernames)} usernames")
        
        # Collect metadata
        results = await self.collect_batch_metadata(usernames)
        
        # Save results
        self.save_results(results, output_file)
        
        # Log summary
        successful = sum(1 for r in results if r["collection_status"] == "success")
        failed = len(results) - successful
        
        logger.info(f"Collection complete: {successful} successful, {failed} failed")


async def main(
    input_file: str = "data/usernames.txt",
    output_file: str = "data/metadata.csv",
    config_path: str = "config/config.yaml"
):
    """Main entry point for the metadata collector."""
    async with BlueskyMetadataCollector(config_path) as collector:
        await collector.run_collection(input_file, output_file)


if __name__ == "__main__":
    fire.Fire(main)

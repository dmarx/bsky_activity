# File: scripts/fetch_bluesky_activity.py
# Fetches social activity from Bluesky and stores it locally
import os
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import fire
from atproto import Client
from loguru import logger
from omegaconf import DictConfig, OmegaConf

class BlueskyActivityFetcher:
    """Fetches and stores Bluesky social activity data."""
    
    def __init__(self, config_path: str = "config/bluesky_config.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.client = Client()
        self.data_dir = Path(self.config.storage.data_directory)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Authenticate the client
        self._authenticate_client()
        
    def _authenticate_client(self) -> None:
        """Authenticate the Bluesky client with credentials."""
        username = self.config.bluesky.get('username', '')
        password = self.config.bluesky.get('password', '')

        username = username if username else os.getenv('BLUESKY_USERNAME')
        password = password if password else os.getenv('BLUESKY_PASSWORD')
        
        if not username or not password:
            logger.error("Bluesky username and password must be provided via environment variables")
            logger.error("Set BLUESKY_USERNAME and BLUESKY_PASSWORD environment variables")
            raise ValueError("Missing Bluesky authentication credentials")
        
        try:
            logger.info(f"Authenticating with Bluesky as {username}")
            self.client.login(username, password)
            logger.info("Successfully authenticated with Bluesky")
        except Exception as e:
            logger.error(f"Failed to authenticate with Bluesky: {e}")
            logger.error("Make sure your username and app password are correct")
            raise
        
    def _load_config(self) -> DictConfig:
        """Load configuration from YAML file with environment variable resolution."""
        if not self.config_path.exists():
            logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        config = OmegaConf.load(self.config_path)
        logger.info(f"Loaded config from {self.config_path}")
        return config
    
    def _get_output_filename(self) -> Path:
        """Generate timestamped output filename."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return self.data_dir / f"bluesky_activity_{timestamp}.json"
    
    def _get_latest_filename(self) -> Path:
        """Get the latest activity filename."""
        return self.data_dir / "bluesky_activity_latest.json"
    
    def _get_last_fetch_timestamp(self) -> datetime | None:
        """Get the timestamp of the most recent post from the last data file."""
        latest_file = self._get_latest_filename()
        
        if not latest_file.exists():
            logger.info("No previous data found, will fetch from lookback period")
            return None
            
        try:
            with latest_file.open('r', encoding='utf-8') as f:
                previous_data = json.load(f)
            
            posts = previous_data.get('posts', [])
            if not posts:
                logger.info("No posts in previous data, will fetch from lookback period")
                return None
            
            # Find the most recent post timestamp
            latest_timestamp = None
            for post in posts:
                post_time = datetime.fromisoformat(post['created_at'].replace('Z', '+00:00'))
                if latest_timestamp is None or post_time > latest_timestamp:
                    latest_timestamp = post_time
            
            if latest_timestamp:
                logger.info(f"Last post was at {latest_timestamp}, fetching newer posts")
                return latest_timestamp
            else:
                logger.info("Could not determine last post timestamp")
                return None
                
        except Exception as e:
            logger.warning(f"Error reading previous data: {e}, will fetch from lookback period")
            return None
    
    def _get_fallback_timestamp(self) -> datetime:
        """Get fallback timestamp based on configured lookback hours."""
        lookback_hours = self.config.bluesky.get('lookback_hours', 168)  # 7 days default
        return datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    def fetch_reply_context(self, reply_uri: str) -> dict[str, Any] | None:
        """Fetch the original post that this is a reply to."""
        try:
            # Parse the AT URI to get the post
            response = self.client.get_post_thread(reply_uri)
            
            if response.thread and hasattr(response.thread, 'post'):
                parent_post = response.thread.post
                return {
                    "uri": parent_post.uri,
                    "cid": parent_post.cid,
                    "author": {
                        "did": parent_post.author.did,
                        "handle": parent_post.author.handle,
                        "display_name": getattr(parent_post.author, 'display_name', None),
                    },
                    "text": parent_post.record.text,
                    "created_at": parent_post.record.created_at,
                    "indexed_at": parent_post.indexed_at,
                }
        except Exception as e:
            logger.warning(f"Could not fetch reply context for {reply_uri}: {e}")
            return None
    
    def fetch_profile_posts(self, handle: str, since_timestamp: datetime | None = None) -> list[dict[str, Any]]:
        """Fetch posts since the given timestamp, or recent posts if no timestamp provided."""
        try:
            profile = self.client.get_profile(handle)
            logger.info(f"Found profile: @{profile.handle} ({profile.display_name})")
            
            # Get the author's DID (Decentralized Identifier)
            author_did = profile.did
            
            # Determine the cutoff time
            if since_timestamp is None:
                since_timestamp = self._get_fallback_timestamp()
                logger.info(f"No timestamp provided, using fallback: {since_timestamp}")
            
            # Fetch posts with pagination to get all posts since timestamp
            all_posts = []
            cursor = None
            max_posts = self.config.bluesky.get('max_posts_per_run', 200)
            posts_per_request = min(100, max_posts)  # API typically limits to 100 per request
            
            while len(all_posts) < max_posts:
                # Make the API request
                if cursor:
                    posts_response = self.client.get_author_feed(
                        actor=author_did,
                        limit=posts_per_request,
                        cursor=cursor
                    )
                else:
                    posts_response = self.client.get_author_feed(
                        actor=author_did,
                        limit=posts_per_request
                    )
                
                if not posts_response.feed:
                    logger.info("No more posts available")
                    break
                
                # Process posts and check timestamps
                new_posts_in_batch = 0
                for post in posts_response.feed:
                    post_time = datetime.fromisoformat(post.post.record.created_at.replace('Z', '+00:00'))
                    
                    # Stop if we've reached posts older than our cutoff
                    if post_time <= since_timestamp:
                        logger.info(f"Reached posts older than cutoff ({since_timestamp}), stopping")
                        break
                    
                    post_data = {
                        "uri": post.post.uri,
                        "cid": post.post.cid,
                        "author": {
                            "did": post.post.author.did,
                            "handle": post.post.author.handle,
                            "display_name": getattr(post.post.author, 'display_name', None),
                        },
                        "text": post.post.record.text,
                        "created_at": post.post.record.created_at,
                        "reply_count": getattr(post.post, 'reply_count', 0),
                        "repost_count": getattr(post.post, 'repost_count', 0),
                        "like_count": getattr(post.post, 'like_count', 0),
                        "indexed_at": post.post.indexed_at,
                    }
                    
                    # Check if this is a reply and fetch context if configured
                    if (hasattr(post.post.record, 'reply') and 
                        post.post.record.reply and 
                        self.config.bluesky.get('include_reply_context', False)):
                        
                        reply_to_uri = post.post.record.reply.parent.uri
                        reply_context = self.fetch_reply_context(reply_to_uri)
                        if reply_context:
                            post_data["reply_context"] = reply_context
                            post_data["is_reply"] = True
                        else:
                            post_data["is_reply"] = True
                            post_data["reply_to_uri"] = reply_to_uri
                    else:
                        post_data["is_reply"] = False
                    
                    # Add embed data if present (images, links, etc.)
                    if hasattr(post.post.record, 'embed') and post.post.record.embed:
                        post_data["embed"] = {
                            "type": post.post.record.embed.py_type,
                            # Add more embed details as needed
                        }
                    
                    all_posts.append(post_data)
                    new_posts_in_batch += 1
                
                # If we didn't find any new posts in this batch, we're done
                if new_posts_in_batch == 0:
                    logger.info("No new posts found in this batch, stopping")
                    break
                
                # Check if we have a cursor for the next page
                cursor = getattr(posts_response, 'cursor', None)
                if not cursor:
                    logger.info("No more pages available")
                    break
                
                logger.info(f"Fetched {new_posts_in_batch} new posts, total: {len(all_posts)}")
            
            logger.info(f"Fetched {len(all_posts)} new posts for @{handle} since {since_timestamp}")
            return all_posts
            
        except Exception as e:
            logger.error(f"Error fetching posts for @{handle}: {e}")
            raise
    
    def fetch_follower_metadata(self, handle: str, limit: int = 1000) -> dict[str, Any]:
        """Fetch detailed metadata about followers."""
        try:
            profile = self.client.get_profile(handle)
            author_did = profile.did
            
            logger.info(f"Fetching followers for @{handle}")
            
            # Get followers
            followers_response = self.client.get_followers(actor=author_did, limit=limit)
            
            follower_metadata = {
                "profile_handle": handle,
                "profile_did": author_did,
                "total_followers_count": getattr(profile, 'followers_count', 0),
                "total_following_count": getattr(profile, 'follows_count', 0),
                "followers": [],
                "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            # Process each follower
            for follower in followers_response.followers:
                follower_data = {
                    "did": follower.did,
                    "handle": follower.handle,
                    "display_name": getattr(follower, 'display_name', None),
                    "created_at": getattr(follower, 'created_at', None),
                    "description": getattr(follower, 'description', None),
                    "followers_count": getattr(follower, 'followers_count', 0),
                    "follows_count": getattr(follower, 'follows_count', 0),
                    "posts_count": getattr(follower, 'posts_count', 0),
                }
                
                # Check if I follow them back
                try:
                    # This requires checking the relationship
                    relationship = self._check_follow_relationship(author_did, follower.did)
                    follower_data.update(relationship)
                except Exception as e:
                    logger.warning(f"Could not check relationship with {follower.handle}: {e}")
                    follower_data["mutual_follow"] = None
                    follower_data["i_follow_them"] = None
                    follower_data["follow_date"] = None
                
                follower_metadata["followers"].append(follower_data)
            
            logger.info(f"Fetched metadata for {len(follower_metadata['followers'])} followers")
            return follower_metadata
            
        except Exception as e:
            logger.error(f"Error fetching follower metadata for @{handle}: {e}")
            raise
    
    def _check_follow_relationship(self, my_did: str, their_did: str) -> dict[str, Any]:
        """Check the follow relationship between two users."""
        try:
            # Get my following list to see if I follow them
            following_response = self.client.get_follows(actor=my_did, limit=100)
            
            i_follow_them = False
            follow_date = None
            
            for follow in following_response.follows:
                if follow.did == their_did:
                    i_follow_them = True
                    follow_date = getattr(follow, 'created_at', None)
                    break
            
            return {
                "i_follow_them": i_follow_them,
                "mutual_follow": i_follow_them,  # Since they already follow me
                "follow_date": follow_date,
            }
            
        except Exception as e:
            logger.warning(f"Error checking follow relationship: {e}")
            return {
                "i_follow_them": None,
                "mutual_follow": None,
                "follow_date": None,
            }
    
    def fetch_activity(self) -> dict[str, Any]:
        """Fetch all configured social activity incrementally."""
        activity_data = {
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            "profile_handle": self.config.bluesky.profile_handle,
            "posts": [],
            "followers_metadata": None,
        }
        
        try:
            # Get the timestamp of the last fetched post
            last_timestamp = self._get_last_fetch_timestamp()
            
            # Fetch posts incrementally
            posts = self.fetch_profile_posts(
                handle=self.config.bluesky.profile_handle,
                since_timestamp=last_timestamp
            )
            activity_data["posts"] = posts
            
            # Add metadata about the fetch
            activity_data["incremental_fetch"] = {
                "last_post_timestamp": last_timestamp.isoformat() if last_timestamp else None,
                "new_posts_count": len(posts),
                "is_initial_fetch": last_timestamp is None
            }
            
            # Fetch follower metadata if configured
            if self.config.bluesky.get('fetch_followers', False):
                logger.info("Fetching follower metadata...")
                followers_metadata = self.fetch_follower_metadata(
                    handle=self.config.bluesky.profile_handle,
                    limit=self.config.bluesky.get('followers_limit', 1000)
                )
                activity_data["followers_metadata"] = followers_metadata
            
            logger.info(f"Successfully fetched activity data: {len(posts)} new posts")
            return activity_data
            
        except Exception as e:
            logger.error(f"Error fetching activity: {e}")
            raise
    
    def save_activity_data(self, data: dict[str, Any]) -> tuple[Path, Path]:
        """Save activity data to timestamped and latest files."""
        # Save timestamped version
        timestamped_file = self._get_output_filename()
        with timestamped_file.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Save as latest
        latest_file = self._get_latest_filename()
        with latest_file.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved activity data to {timestamped_file} and {latest_file}")
        return timestamped_file, latest_file
    
    def cleanup_old_files(self, keep_count: int | None = None) -> None:
        """Remove old timestamped files, keeping only the most recent ones."""
        if keep_count is None:
            keep_count = self.config.storage.get("keep_files", 10)
        
        # Find all timestamped files
        pattern = "bluesky_activity_*.json"
        files = list(self.data_dir.glob(pattern))
        
        # Exclude the latest file from cleanup
        latest_file = self._get_latest_filename()
        files = [f for f in files if f.name != latest_file.name]
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Remove old files
        files_to_remove = files[keep_count:]
        for file_path in files_to_remove:
            file_path.unlink()
            logger.info(f"Removed old file: {file_path}")
        
        if files_to_remove:
            logger.info(f"Cleaned up {len(files_to_remove)} old files")
    
    def run(self, cleanup: bool = True) -> None:
        """Main execution method."""
        logger.info("Starting Bluesky activity fetch")
        
        try:
            # Fetch activity data
            activity_data = self.fetch_activity()
            
            # Save the data
            timestamped_file, latest_file = self.save_activity_data(activity_data)
            
            # Cleanup old files if requested
            if cleanup:
                self.cleanup_old_files()
            
            logger.info("Bluesky activity fetch completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to fetch Bluesky activity: {e}")
            raise


def main():
    """CLI entry point using Fire."""
    fire.Fire(BlueskyActivityFetcher)


if __name__ == "__main__":
    main()
    """Fetches and stores Bluesky social activity data."""
    
    def __init__(self, config_path: str = "config/bluesky_config.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.client = Client()
        self.data_dir = Path(self.config.storage.data_directory)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self) -> DictConfig:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        config = OmegaConf.load(self.config_path)
        logger.info(f"Loaded config from {self.config_path}")
        return config
    
    def _get_output_filename(self) -> Path:
        """Generate timestamped output filename."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return self.data_dir / f"bluesky_activity_{timestamp}.json"
    
    def _get_latest_filename(self) -> Path:
        """Get the latest activity filename."""
        return self.data_dir / "bluesky_activity_latest.json"
    
    def fetch_profile_posts(self, handle: str, limit: int = 50) -> list[dict[str, Any]]:
        """Fetch recent posts from a Bluesky profile."""
        try:
            profile = self.client.get_profile(handle)
            logger.info(f"Found profile: @{profile.handle} ({profile.display_name})")
            
            # Get the author's DID (Decentralized Identifier)
            author_did = profile.did
            
            # Fetch posts by this author
            posts_response = self.client.get_author_feed(
                actor=author_did,
                limit=limit
            )
            
            posts = []
            for post in posts_response.feed:
                post_data = {
                    "uri": post.post.uri,
                    "cid": post.post.cid,
                    "author": {
                        "did": post.post.author.did,
                        "handle": post.post.author.handle,
                        "display_name": getattr(post.post.author, 'display_name', None),
                    },
                    "text": post.post.record.text,
                    "created_at": post.post.record.created_at,
                    "reply_count": getattr(post.post, 'reply_count', 0),
                    "repost_count": getattr(post.post, 'repost_count', 0),
                    "like_count": getattr(post.post, 'like_count', 0),
                    "indexed_at": post.post.indexed_at,
                }
                
                # Add embed data if present (images, links, etc.)
                if hasattr(post.post.record, 'embed') and post.post.record.embed:
                    post_data["embed"] = {
                        "type": post.post.record.embed.py_type,
                        # Add more embed details as needed
                    }
                
                posts.append(post_data)
            
            logger.info(f"Fetched {len(posts)} posts for @{handle}")
            return posts
            
        except Exception as e:
            logger.error(f"Error fetching posts for @{handle}: {e}")
            raise
    
    def fetch_activity(self) -> dict[str, Any]:
        """Fetch all configured social activity."""
        activity_data = {
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            "profile_handle": self.config.bluesky.profile_handle,
            "posts": [],
        }
        
        try:
            # Fetch posts
            posts = self.fetch_profile_posts(
                handle=self.config.bluesky.profile_handle,
                limit=self.config.bluesky.posts_limit
            )
            activity_data["posts"] = posts
            
            logger.info(f"Successfully fetched activity data")
            return activity_data
            
        except Exception as e:
            logger.error(f"Error fetching activity: {e}")
            raise
    
    def save_activity_data(self, data: dict[str, Any]) -> tuple[Path, Path]:
        """Save activity data to timestamped and latest files."""
        # Save timestamped version
        timestamped_file = self._get_output_filename()
        with timestamped_file.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Save as latest
        latest_file = self._get_latest_filename()
        with latest_file.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved activity data to {timestamped_file} and {latest_file}")
        return timestamped_file, latest_file
    
    def cleanup_old_files(self, keep_count: int | None = None) -> None:
        """Remove old timestamped files, keeping only the most recent ones."""
        if keep_count is None:
            keep_count = self.config.storage.get("keep_files", 10)
        
        # Find all timestamped files
        pattern = "bluesky_activity_*.json"
        files = list(self.data_dir.glob(pattern))
        
        # Exclude the latest file from cleanup
        latest_file = self._get_latest_filename()
        files = [f for f in files if f.name != latest_file.name]
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Remove old files
        files_to_remove = files[keep_count:]
        for file_path in files_to_remove:
            file_path.unlink()
            logger.info(f"Removed old file: {file_path}")
        
        if files_to_remove:
            logger.info(f"Cleaned up {len(files_to_remove)} old files")
    
    def run(self, cleanup: bool = True) -> None:
        """Main execution method."""
        logger.info("Starting Bluesky activity fetch")
        
        try:
            # Fetch activity data
            activity_data = self.fetch_activity()
            
            # Save the data
            timestamped_file, latest_file = self.save_activity_data(activity_data)
            
            # Cleanup old files if requested
            if cleanup:
                self.cleanup_old_files()
            
            logger.info("Bluesky activity fetch completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to fetch Bluesky activity: {e}")
            raise


def main():
    """CLI entry point using Fire."""
    fire.Fire(BlueskyActivityFetcher)


if __name__ == "__main__":
    main()

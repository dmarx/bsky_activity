# Python Project Structure

## scripts/fetch_bluesky_activity.py
```python
class BlueskyActivityFetcher
    """Fetches and stores Bluesky social activity data."""

    def __init__(self, config_path: str)

    def _authenticate_client(self) -> None
        """Authenticate the Bluesky client with credentials."""

    def _load_config(self) -> DictConfig
        """Load configuration from YAML file with environment variable resolution."""

    def _get_output_filename(self) -> Path
        """Generate timestamped output filename."""

    def _get_latest_filename(self) -> Path
        """Get the latest activity filename."""

    def _get_last_fetch_timestamp(self) -> datetime | None
        """Get the timestamp of the most recent post from the last data file."""

    def _get_fallback_timestamp(self) -> datetime
        """Get fallback timestamp based on configured lookback hours."""

    def fetch_reply_context(self, reply_uri: str) -> dict[[str, Any]] | None
        """Fetch the original post that this is a reply to."""

    def fetch_profile_posts(self, handle: str, since_timestamp: datetime | None) -> list[dict[[str, Any]]]
        """Fetch posts since the given timestamp, or recent posts if no timestamp provided."""

    def fetch_follower_metadata(self, handle: str, limit: int) -> dict[[str, Any]]
        """Fetch detailed metadata about followers."""

    def _check_follow_relationship(self, my_did: str, their_did: str) -> dict[[str, Any]]
        """Check the follow relationship between two users."""

    def fetch_activity(self) -> dict[[str, Any]]
        """Fetch all configured social activity incrementally."""

    def save_activity_data(self, data: dict[[str, Any]]) -> tuple[[Path, Path]]
        """Save activity data to timestamped and latest files."""

    def cleanup_old_files(self, keep_count: int | None) -> None
        """Remove old timestamped files, keeping only the most recent ones."""

    def run(self, cleanup: bool) -> None
        """Main execution method."""


def main()
    """CLI entry point using Fire."""

def main()
    """CLI entry point using Fire."""

def __init__(self, config_path: str)

def _load_config(self) -> DictConfig
    """Load configuration from YAML file."""

def _get_output_filename(self) -> Path
    """Generate timestamped output filename."""

def _get_latest_filename(self) -> Path
    """Get the latest activity filename."""

def fetch_profile_posts(self, handle: str, limit: int) -> list[dict[[str, Any]]]
    """Fetch recent posts from a Bluesky profile."""

def fetch_activity(self) -> dict[[str, Any]]
    """Fetch all configured social activity."""

def save_activity_data(self, data: dict[[str, Any]]) -> tuple[[Path, Path]]
    """Save activity data to timestamped and latest files."""

def cleanup_old_files(self, keep_count: int | None) -> None
    """Remove old timestamped files, keeping only the most recent ones."""

def run(self, cleanup: bool) -> None
    """Main execution method."""

```

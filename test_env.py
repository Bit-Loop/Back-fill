#!/usr/bin/env python3
"""
Test script to verify .env configuration is loaded correctly.
Run this to check if all required API keys and settings are present.
"""
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from common.config.settings import BackfillConfig

def check_env_var(name: str, required: bool = True) -> bool:
    """Check if an environment variable is set."""
    value = os.getenv(name)
    if value:
        # Mask sensitive values
        if 'KEY' in name or 'PASSWORD' in name or 'SECRET' in name:
            display = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
        else:
            display = value
        print(f"✅ {name:30s} = {display}")
        return True
    else:
        status = "❌ MISSING (Required)" if required else "⚠️  Not set (Optional)"
        print(f"{status:10s} {name}")
        return not required

def main():
    """Check all environment variables."""
    print("=" * 70)
    print("Environment Configuration Check")
    print("=" * 70)
    print()
    
    # Load .env
    load_dotenv()
    print("📄 Loading configuration from .env file...")
    print()
    
    all_ok = True
    
    print("Required Configuration:")
    print("-" * 70)
    all_ok &= check_env_var('POLYGON_API_KEY', required=True)
    all_ok &= check_env_var('TIMESCALE_HOST', required=True)
    all_ok &= check_env_var('TIMESCALE_PORT', required=True)
    all_ok &= check_env_var('TIMESCALE_DB', required=True)
    all_ok &= check_env_var('TIMESCALE_USER', required=True)
    all_ok &= check_env_var('TIMESCALE_PASSWORD', required=True)
    all_ok &= check_env_var('REDIS_HOST', required=True)
    all_ok &= check_env_var('REDIS_PORT', required=True)
    
    print()
    print("Optional Configuration:")
    print("-" * 70)
    check_env_var('REDIS_PASSWORD', required=False)
    check_env_var('POLYGON_S3_ACCESS_KEY', required=False)
    check_env_var('POLYGON_S3_SECRET_KEY', required=False)
    check_env_var('LOG_DIR', required=False)
    check_env_var('LOG_LEVEL', required=False)
    check_env_var('SCRAPER_HOST', required=False)
    check_env_var('SCRAPER_PORT', required=False)
    
    print()
    print("=" * 70)
    print("Configuration Object Test:")
    print("-" * 70)
    
    try:
        config = BackfillConfig.default()
        print(f"✅ BackfillConfig loaded successfully")
        print(f"   Polygon API Key: {'SET' if config.polygon.api_key else 'NOT SET'}")
        print(f"   Database: {config.database.host}:{config.database.port}/{config.database.database}")
        print(f"   Redis: {config.redis.host}:{config.redis.port}")
    except Exception as e:
        print(f"❌ Failed to load BackfillConfig: {e}")
        all_ok = False
    
    print()
    print("=" * 70)
    if all_ok:
        print("✅ All required configuration is set!")
        print("   You can now run: python scraper.py --tickers AAPL --years 1")
        return 0
    else:
        print("❌ Some required configuration is missing!")
        print("   Please check your .env file and add the missing values.")
        print("   See .env.example for reference.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

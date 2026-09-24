"""
FPS.ms entry point. Wraps main.py so the platform can run our bot.
"""
import asyncio
from main import main

if __name__ == "__main__":
    asyncio.run(main())

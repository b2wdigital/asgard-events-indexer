import asyncio

from indexer.app import main

asyncio.get_event_loop().run_until_complete(main())

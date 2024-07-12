```python
import asyncio
from src.lib_cashmere.collection import CashmereCollection
from src.apps.demo_one.cashmere import cashmere as demo_one_cashmere
from src.apps.demo_two.cashmere import cashmere as demo_two_cashmere

def weave() -> None:
    
    cashmere_apps = [demo_one_cashmere, demo_two_cashmere]

    cashmere_collection = CashmereCollection(
        namespace_prefix='dev', apps=cashmere_apps
    )

    asyncio.run(cashmere_collection.thread_the_loom())
    asyncio.run(cashmere_collection.weave())
```
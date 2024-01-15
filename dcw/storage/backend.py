from pathlib import PurePath
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, Union


class StorageBackend(Protocol):
    def write(self, path: Union[str, PurePath], data: Any = None, *, func: Optional[Callable] = None,
              args: Optional[Union[Tuple, List]] = None, kwargs: Optional[Dict[str, Any]] = None) -> None:
        pass

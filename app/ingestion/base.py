from abc import ABC, abstractmethod
from typing import List

class NewsSource(ABC):
    @abstractmethod
    def fetch_headlines(self) -> List[str]:
        pass

import typing 
from abc import ABC, abstarctmethod

class DataProcessor(ABC):
    pass


    @abstarctmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstarctmethod
    def ingest(self, data: Any) -> None:
        pass


class NumericProcessor(DataProcessor):
    pass

class TextProcessor(DataProcessor):
    pass

class LogProcessor(DataProcessor):
    pass

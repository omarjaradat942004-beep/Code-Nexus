from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    def __init__(self) -> None:
        self.storage: list[str] = []
        self.rank: int = -1

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        self.rank += 1
        self.last_item: str = self.storage.pop(0)
        return (self.rank, self.last_item)


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True

        elif isinstance(data, list):
            for index in data:
                if (
                    not isinstance(index, (int, float))
                    or isinstance(index, bool)
                ):
                    return False
            return True

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError()

        if isinstance(data, (int, float)) and not isinstance(data, bool):
            self.storage.append(str(data))

        elif isinstance(data, list):
            for index in data:
                self.storage.append(str(index))


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        elif isinstance(data, list):
            for index in data:
                if not isinstance(index, str):
                    return False
            return True

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError()

        if isinstance(data, str):
            self.storage.append(data)

        elif isinstance(data, list):
            for index in data:
                self.storage.append(index)


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for keys, values in data.items():
                if not isinstance(keys, str) or not isinstance(values, str):
                    return False
            return True

        elif isinstance(data, list):
            for index in data:
                if not isinstance(index, dict):
                    return False

                for keys, values in index.items():
                    if (
                        not isinstance(keys, str)
                        or not isinstance(values, str)
                    ):
                        return False

            return True

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError()

        if isinstance(data, dict):
            self.storage.append(str(data))

        elif isinstance(data, list):
            for index in data:
                self.storage.append(str(index))

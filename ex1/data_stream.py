from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    def __init__(self) -> None:
        self.storage: list[str] = []
        self.rank: int = -1
        self.total_count: int = 0
        self.remaining: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        self.rank += 1
        self.last_item: str = self.storage.pop(0)
        self.remaining -= 1
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
            self.total_count += 1
            self.remaining += 1

        elif isinstance(data, list):
            for index in data:
                self.storage.append(str(index))
                self.total_count += 1
                self.remaining += 1


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
            self.total_count += 1
            self.remaining += 1

        elif isinstance(data, list):
            for index in data:
                self.storage.append(index)
                self.total_count += 1
                self.remaining += 1


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
            self.total_count += 1
            self.remaining += 1

        elif isinstance(data, list):
            for index in data:
                self.storage.append(str(index))
                self.total_count += 1
                self.remaining += 1


class DataStream:

    def __init__(self) -> None:
        self.processors_list: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors_list.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        self.stream = stream
        for index in self.stream:
            self.clean: bool = False
            for index2 in self.processors_list:
                if (index2.validate(index)):
                    index2.ingest(index)
                    self.clean = True
            if (not self.clean):
                print("Error")

        if (len(self.processors_list) == 0):
            print("No processor found, no data")

    def print_processors_stats(self) -> None:
        for index in self.processors_list:
            if (isinstance(index, NumericProcessor)):
                print(f"Numeric Processor: total {index.total_count}"
                      f" items processed, remaining"
                      f" {index.remaining} on processor")
            elif (isinstance(index, TextProcessor)):
                print(f"Text Processor: total {index.total_count}"
                      f" items processed, remaining"
                      f" {index.remaining} on processor")
            else:
                print(f"Log Processor: total {index.total_count}"
                      f" items processed, remaining"
                      f" {index.remaining} on processor")

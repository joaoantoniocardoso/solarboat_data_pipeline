#!/usr/bin/env python
# coding: utf-8

import ctypes
from typing import Tuple, List
import itertools


class CanTopicParser:
    @staticmethod
    def generate_parsers(schema: dict, inline=True) -> dict:
        """
        Generate a parser for each topic of the schema and add the parsers to the schema.

        Usage:
            parsed_data = schema[module][topic]['parser'].from_bytes(bytearray(data))
        """
        if not inline:
            schema = schema.copy()
        for module in schema["modules"]:
            for topic in schema["modules"][module]["topics"]:
                schema["modules"][module]["topics"][topic][
                    "parser"
                ] = CanTopicParser.create(
                    schema["modules"][module]["name"],
                    schema["modules"][module]["topics"][topic],
                )

        return schema

    @staticmethod
    def create(module_name: str, topic: dict) -> ctypes.LittleEndianStructure:
        """
        Create a ctypes structure for the given topic.

        Usage:
            my_data = b'\x00\x01\x02\x03\x04'
            topic, module = 33, 250
            my_topic = json.load('can_ids.json')[module][topic]

            my_topic_parser = CanTopicParser.create(my_topic)
            my_data_parsed = my_topic_parser.from_buffer(bytearray(my_data))
            print(my_data_parsed)
        """
        name = f"{module_name}.{topic['name']}"
        return type(
            name,
            (ctypes.LittleEndianStructure,),
            {
                "_pack_": 1,
                "_fields_": CanTopicParser._fields_from_topic(topic),
                "__repr__": CanTopicParser._repr,
                "__str__": CanTopicParser._str,
                "size": CanTopicParser._size_from_topic(topic),
                "as_dict": CanTopicParser._as_dict,
            },
        )  # type: ignore

    @staticmethod
    def apply_units(units: str, value: float) -> Tuple[str, float]:
        """

        Note that if the units string is not in the expected format, the function will raise a ValueError when trying to convert the value to a float

        """
        if units == "%":
            scale = 1 / 255
            value *= scale
        elif units != "":
            _units = [
                "".join(x)
                for _, x in itertools.groupby(iterable=units, key=str.isdigit)
            ]
            scale = 1 / float(_units[1])
            units = _units[0].replace("/", "")
            value *= scale

        return units, value

    @staticmethod
    def _fields_from_topic(topic: dict) -> list:
        _ctypes_map = {
            # Converts the 'type' from can_ids.json to ctypes:
            "u8": (ctypes.c_uint8, 8),  # old format
            "u16": (ctypes.c_uint16, 16),  # old format
            "uint8_t": (ctypes.c_uint8, 8),
            "uint16_t": (ctypes.c_uint16, 16),
            "bitfield": (ctypes.c_uint8, 1),
        }
        fields = []
        for byte in topic["bytes"]:
            if not byte:
                continue

            byte_name = byte["name"]
            if byte_name.endswith("_H"):
                continue
            elif byte_name.endswith("_L"):
                byte_name = byte_name[:-2]
            byte_type = byte["type"]

            fields.append((byte_name, *_ctypes_map[byte_type]))

        return fields

    def _as_dict(self) -> dict:
        return dict((k, getattr(self, k)) for k, *_ in self._fields_)  # type: ignore

    def _repr(self) -> str:
        return self.as_dict().__repr__()  # type: ignore

    def _str(self) -> str:
        return self.as_dict().__str__()  # type: ignore

    @staticmethod
    def _size_from_topic(topic) -> int:
        fields = CanTopicParser._fields_from_topic(topic)
        return sum([ctypes.sizeof(f[1]) for f in fields])

from killuhub.processing.spark_engine import SparkEngine
from killuhub.processing.flink_engine import FlinkEngine
from killuhub.core.registry import default_registry

default_registry.register_engine("spark", SparkEngine)
default_registry.register_engine("flink", FlinkEngine)

__all__ = ["SparkEngine", "FlinkEngine"]

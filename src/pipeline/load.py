from pyspark.sql import DataFrame, functions as F

from src.pipeline.exceptions import PipelineWriteError

def load(
    df: DataFrame,
    path: str,
    write_mode:str ="overwrite",
    partition_columns: tuple[str] = ()
) -> None:

    try:
        if partition_columns:
            df.write.mode(write_mode).partitionBy(partition_columns).parquet(path)
        else:
            df.write.mode(write_mode).parquet(path)
    
    except Exception as e:
        raise PipelineWriteError(
            f"Failed to write Parquet to: {path}"
        ) from e
    
    return
    
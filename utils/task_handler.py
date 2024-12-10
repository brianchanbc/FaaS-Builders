import uuid
from utils.serializer import deserialize, serialize
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def execute_fn(task_id: uuid.UUID, ser_fn: str, ser_params: str):
    logger.info(f"Preparing to execute task {task_id}")
    
    # deserialize function and params
    fn = deserialize(ser_fn)
    args, kwargs = deserialize(ser_params)
    logger.info(f"Deserialized function and params")
    
    # execute the function..
    logger.info(f"Executing function {fn.__name__}")
    result = fn(*args, **kwargs)
    logger.info(f"Function {fn.__name__} executed")
    
    # serialize and return the results
    logger.info(f"Serialize result and send back")
    return serialize(result)
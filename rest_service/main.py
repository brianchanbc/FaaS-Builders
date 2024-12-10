from fastapi import FastAPI, HTTPException
import redis
import uuid

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from utils.serializer import deserialize, serialize
from utils.models import RegisterFn, RegisterFnRep, ExecuteFnReq, ExecuteFnRep, TaskStatusRep, TaskResultRep
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI()

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
redis_client.flushall()

@app.post("/register_function")
async def register(fn: RegisterFn) -> RegisterFnRep:
    fn_id = uuid.uuid4()
    fn_key = f"function:{fn_id}"
    
    # Check empty name and payload
    if not fn.name.strip() or not fn.payload.strip():
        logger.error("Empty name or payload provided")
        raise HTTPException(status_code=400, detail="Name and payload must not be empty")
    
    # Check if payload is serialized    
    try:
        obj = deserialize(fn.payload)
        if not callable(obj):
            logger.error("Provided payload is not callable")
            raise HTTPException(status_code=400, detail="Payload is not callable")
    except Exception as e:
        logger.error(f"Invalid payload: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid function payload: {str(e)}")
    
    # Check if key already exists in Redis
    if redis_client.exists(fn_key):
        logger.error("Function already exists")
        raise HTTPException(status_code=400, detail="Function already exists")
    
    try:
        # Try to insert function into Redis
        redis_client.hset(
            fn_key,
            mapping={
                "name": fn.name,
                "payload": fn.payload,
            }
        )
        logger.info(f"Assigned function ID: {fn_id}")
        return RegisterFnRep(function_id=fn_id)
    except Exception as e:
        logger.error(f"Redis error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/status/{task_id}")
async def status(task_id: uuid.UUID) -> TaskStatusRep:
    task_key = f"task:{task_id}"
    
    # Check if task key exists
    if not redis_client.exists(task_key):
        logger.error("Task not found")
        raise HTTPException(status_code=404, detail="Task not found")
    
    try:
        # Try to get task status from Redis
        status = redis_client.hget(task_key, "status")
        logger.info(f"Task {task_id} status: {status}")
        return TaskStatusRep(task_id=task_id, status=status)
    except Exception as e:
        logger.error(f"Redis error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute_function")
async def execute_function(req: ExecuteFnReq) -> ExecuteFnRep:
    task_id = uuid.uuid4()
    task_key = f"task:{task_id}"
    fn_key = f"function:{req.function_id}"
    
    # Check if the function exists in Redis
    if not redis_client.exists(fn_key):
        logger.error(f"Function {req.function_id} not found")
        raise HTTPException(status_code=404, detail="Function not found")
    
    try:
        # Retrieve the function payload from Redis
        fn_payload = redis_client.hget(fn_key, "payload")
        if not fn_payload:
            logger.error(f"Function {req.function_id} has no payload")
            raise HTTPException(status_code=500, detail="Function payload not found")
        
    except Exception as e:
        logger.error(f"Error retrieving function: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing function payload")
    
    try:
        # Create the task entry in Redis
        redis_client.hset(
            task_key,
            mapping={
                "function_id": str(req.function_id),
                "param_payload": req.payload,
                "status": "QUEUED",  # Initial status
                "result": ""          # No result yet
            }
        )
        
        # Publish the task to the Redis pub/sub channel
        redis_client.publish("Tasks", serialize({"task_id": str(task_id)}))
        
        logger.info(f"Task {task_id} created for function {req.function_id}")
        return ExecuteFnRep(task_id=task_id)
    except Exception as e:
        logger.error(f"Redis error while creating task: {str(e)}")
        raise HTTPException(status_code=500, detail="Error creating task")

@app.get("/result/{task_id}")
async def get_task_result(task_id: uuid.UUID) -> TaskResultRep:
    task_key = f"task:{task_id}"
    
    # Check if the task exists in Redis
    if not redis_client.exists(task_key):
        logger.error(f"Task {task_id} not found")
        raise HTTPException(status_code=404, detail="Task not found")
    
    try:
        # Retrieve the task status and result from Redis
        status = redis_client.hget(task_key, "status")
        result = redis_client.hget(task_key, "result")
        
        # Only deserialize if result is not empty
        deserialized_result = deserialize(result) if result else None
        
        logger.info(f"Task {task_id} status: {status}, result: {deserialized_result}")
        return TaskResultRep(task_id=task_id, status=status, result=result)
    except Exception as e:
        logger.error(f"Redis error while retrieving task result: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving task result")

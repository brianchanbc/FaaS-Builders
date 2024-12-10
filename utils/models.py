from dataclasses import dataclass, field
from enum import Enum
from pydantic import BaseModel
import uuid
from typing import Set

class RegisterFn(BaseModel):
    name: str
    payload: str

class RegisterFnRep(BaseModel):
    function_id: uuid.UUID
    
class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str
    
class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID

class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str

class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str

@dataclass
class Task:
    task_id: str
    task_start_time: float
    task_time_out: float
    fn: str
    params: str


from typing import Any, Dict

from sequor.core.op import Op
from sequor.core.user_error import UserError

from sequor.source.source import Source
from sequor.source.sources.duckdb_source import DuckDBSource
from sequor.source.sources.http_source import HTTPSource
from sequor.source.sources.sql_source import SQLSource


def create_source(context: 'Context', source_name: str, source_def: Dict[str, Any]) -> Any:
    source: Source = None
    source_type = source_def.get('type')
    if source_type == 'http':
        source = HTTPSource(context, source_name, source_def)
    elif source_type == 'postgres':
        source = SQLSource(context, source_name, source_def)
    elif source_type == 'duckdb':
        source = DuckDBSource(context, source_name, source_def)
    else:
        raise ValueError(f"Unknown source type: {source_type}")
    return source

# @classmethod
# def create(cls, proj, op_def: Dict[str, Any]) -> 'Op':


def create_op(proj, op_def: Dict[str, Any]) -> 'Op':
    """Factory method to create operation instances"""
    op_type = op_def.get('op')
    op: Op = None
    if op_type == "http_request":
        from sequor.operations.http_request import HTTPRequestOp
        op = HTTPRequestOp(proj, op_def)
    elif op_type == "transform":
        from sequor.operations.transform import TransformOp
        op = TransformOp(proj, op_def)
    elif op_type == "execute":
        from sequor.operations.execute import ExecuteOp
        op = ExecuteOp(proj, op_def)
    elif op_type == "run_flow":
        from sequor.operations.run_flow import RunFlowOp
        op = RunFlowOp(proj, op_def)
    elif op_type == "set_variable":
        from sequor.operations.set_variable import SetVariableOp
        op = SetVariableOp(proj, op_def)
    elif op_type == "print":
        from sequor.operations.print import PrintOp
        op = PrintOp(proj, op_def)
    elif op_type == "if":
        from sequor.operations.if_op import IfOp
        op = IfOp(proj, op_def)
    elif op_type == "for_each":
        from sequor.operations.for_each import ForEachOp
        op = ForEachOp(proj, op_def)
    elif op_type == "block":
        from sequor.operations.block import BlockOp
        op = BlockOp(proj, op_def)
    else:
        raise UserError(f"Unknown operation: {op_type}")
    # if op_type not in cls._registry:
    #     raise ValueError(f"Unknown operation type: {op_type}")
    # return cls._registry[op_type](proj, op_def)
    return op









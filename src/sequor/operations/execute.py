import logging
from typing import Any, Dict

from sequor.common.executor_utils import render_jinja
from sequor.core.op import Op
from sequor.core.user_error import UserError
from sequor.source.table_address import TableAddress


# @Op.register('execute')
class ExecuteOp(Op):
    def __init__(self, proj, op_def: Dict[str, Any]):
        self.name = op_def.get('op')
        self.proj = proj
        self.op_def = op_def

    def get_title(self) -> str:
        op_id = self.op_def.get('id')

        if op_id is not None:
            title = self.name + ": " + op_id
        else:
            title = "unknown"
        return title

    def run(self, context, op_options: Dict[str, Any]):
        logger = logging.getLogger("sequor.ops.transform")
        self.op_def = render_jinja(context, self.op_def)
        logger.info(f"Starting \"{self.get_title()}\"")
        source_name = Op.get_parameter(context, self.op_def, 'source', is_required=True)
        source_name = Op.get_parameter(context, self.op_def, 'source', is_required=True)

        script = self.op_def.get('statement')
        if not script:
            raise UserError("The 'statement' parameter is required and cannot be empty.")

        # Split the script into individual statements using 'go' as a separator
        statements = []
        current_statement = []

        for line in script.splitlines():
            if line.strip().lower() == "go":
                if current_statement:
                    statements.append("\n".join(current_statement).strip())
                    current_statement = []
            else:
                current_statement.append(line)

        # Add the last statement if it exists
        if current_statement:
            statements.append("\n".join(current_statement).strip())

        # Check if the last statement is followed by a 'go' command
        if script.strip().splitlines()[-1].strip().lower() != "go":
            raise UserError(
                "Missing 'go' command after the last statement. Each statement must be followed by a 'go' command on its own line."
            )
    
        source = self.proj.get_source(context, source_name)
        with source.connect() as conn:
            for stmt in statements:
                if stmt.strip() == "": # Skip emty statements (muliple go comands in a row) and the empty statement after the last go
                    continue
                logger.info(f"Executing statement: {stmt}")
                conn.execute_update(stmt)
    
        logger.info(f"Finished \"{self.get_title()}\"")        
"""
Formula Builder for Custom Derived Columns

Security note:
- This module previously used Python eval() to evaluate user-provided expressions.
- That is not safe. This version uses a restricted, AST-based evaluator for:
  - arithmetic expressions
  - boolean conditions
  - string literals and simple concatenation
- It also supports conditional formulas by building nested np.where calls.

Supported expression language (high level):
- Column references: [column_name]
- Numeric literals: 1, 1.23
- String literals: 'text' or "text"
- Arithmetic: +, -, *, /, %, ** (power)
- Comparisons: ==, !=, <, <=, >, >=
- Boolean ops: and, or, not (also works with parentheses)
- For string formulas: allows concatenation via +
- For conditional formulas: conditions are boolean expressions returning a boolean Series.

Unsafe features are rejected:
- attribute access, function calls, indexing, comprehensions, imports, names other than columns
"""

from __future__ import annotations

import ast
import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


_ALLOWED_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow)
_ALLOWED_UNARYOPS = (ast.UAdd, ast.USub, ast.Not)
_ALLOWED_CMPOPS = (
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
)
_ALLOWED_BOOLOPS = (ast.And, ast.Or)


class FormulaValidationError(ValueError):
    pass


class UnsafeExpressionError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedExpression:
    expr: ast.AST
    referenced_columns: Tuple[str, ...]


def _extract_column_tokens(formula: str, df: pd.DataFrame) -> List[str]:
    """
    Extract all [col] references and only keep those existing in df.
    """
    import re

    pattern = r"\[([^\]]+)\]|'([^']+)'|\"([^\"]+)\""
    matches = re.findall(pattern, formula)

    cols: List[str] = []
    for match in matches:
        col = match[0] or match[1] or match[2]
        # Only treat bracket captures as columns; quoted captures are for strings.
        # When regex matches quotes, match[0] is empty.
        if match[0]:
            if col in df.columns:
                cols.append(col)
    return cols


def _replace_column_tokens_with_identifiers(formula: str) -> Tuple[str, List[str]]:
    """
    Replace [col] -> __col_<i> identifiers to keep AST free of bracket syntax.
    Return replaced formula and identifiers mapping in order.
    """
    import re

    tokens = re.findall(r"\[([^\]]+)\]", formula)
    # Deduplicate but preserve order
    seen: set[str] = set()
    ordered_tokens: List[str] = []
    for t in tokens:
        if t not in seen:
            ordered_tokens.append(t)
            seen.add(t)

    mapping: Dict[str, str] = {col: f"__col_{i}" for i, col in enumerate(ordered_tokens)}

    def repl(m: re.Match[str]) -> str:
        col = m.group(1)
        return mapping[col]

    replaced = re.sub(r"\[([^\]]+)\]", repl, formula)
    return replaced, ordered_tokens


def _parse_expression(expression_text: str) -> ParsedExpression:
    try:
        tree = ast.parse(expression_text, mode="eval")
    except SyntaxError as e:
        raise FormulaValidationError(f"Syntax error: {e}") from e

    referenced_columns: List[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced_columns.append(node.id)

    # Uniquify while preserving order
    uniq: List[str] = []
    seen: set[str] = set()
    for name in referenced_columns:
        if name not in seen:
            uniq.append(name)
            seen.add(name)

    return ParsedExpression(expr=tree, referenced_columns=tuple(uniq))


def _validate_expression_safety(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Expression):
            continue

        # Disallow calls entirely
        if isinstance(node, ast.Call):
            raise UnsafeExpressionError("Function calls are not allowed in formulas")

        # Disallow attribute access: x.y
        if isinstance(node, ast.Attribute):
            raise UnsafeExpressionError("Attribute access is not allowed in formulas")

        # Disallow subscripting/indexing: x[0]
        if isinstance(node, ast.Subscript):
            raise UnsafeExpressionError("Indexing is not allowed in formulas")

        # Disallow comprehensions
        if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            raise UnsafeExpressionError("Comprehensions are not allowed in formulas")

        # Allowed operators
        if isinstance(node, ast.BinOp):
            if not isinstance(node.op, _ALLOWED_BINOPS):
                raise UnsafeExpressionError(f"Operator '{type(node.op).__name__}' is not allowed")
        if isinstance(node, ast.UnaryOp):
            if not isinstance(node.op, _ALLOWED_UNARYOPS):
                raise UnsafeExpressionError(f"Unary operator '{type(node.op).__name__}' is not allowed")
        if isinstance(node, ast.BoolOp):
            if not isinstance(node.op, _ALLOWED_BOOLOPS):
                raise UnsafeExpressionError(f"Boolean operator '{type(node.op).__name__}' is not allowed")
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if not isinstance(op, _ALLOWED_CMPOPS):
                    raise UnsafeExpressionError("Comparison operator is not allowed")

        # Allow literals, names, and core expression nodes
        allowed_nodes = (
            ast.Expression,
            ast.BinOp,
            ast.UnaryOp,
            ast.BoolOp,
            ast.Compare,
            ast.Name,
            ast.Load,
            ast.Constant,
            ast.And,
            ast.Or,
            ast.Not,
            ast.Eq,
            ast.NotEq,
            ast.Lt,
            ast.LtE,
            ast.Gt,
            ast.GtE,
            ast.Add,
            ast.Sub,
            ast.Mult,
            ast.Div,
            ast.Mod,
            ast.Pow,
            ast.UAdd,
            ast.USub,
            ast.Not,
            ast.ParenExpr if hasattr(ast, "ParenExpr") else ast.Expr,
        )
        # ast.ParenExpr may not exist in all Python versions
        if not isinstance(node, allowed_nodes):
            # If it's a node type we didn't anticipate, reject.
            raise UnsafeExpressionError(f"Unsupported syntax in formula: {type(node).__name__}")

        # Also disallow any Name that's not a column identifier we produced
        if isinstance(node, ast.Name):
            # must be one of our generated identifiers: __col_<i>
            if not (node.id.startswith("__col_")):
                raise UnsafeExpressionError(f"Unknown identifier '{node.id}'")

    # Basic check: expression shouldn't be empty
    if not isinstance(tree, ast.Expression):
        raise UnsafeExpressionError("Invalid expression")


def _safe_eval(
    tree: ast.AST,
    variables: Dict[str, Any],
) -> Any:
    """
    Evaluate restricted AST nodes against provided variables (pandas Series or scalars).
    """
    def eval_node(node: ast.AST) -> Any:
        if isinstance(node, ast.Expression):
            return eval_node(node.body)

        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            return variables[node.id]

        if isinstance(node, ast.BinOp):
            left = eval_node(node.left)
            right = eval_node(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left**right
            raise UnsafeExpressionError("Binary operator not supported")

        if isinstance(node, ast.UnaryOp):
            operand = eval_node(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.Not):
                # boolean NOT for Series
                return ~operand if isinstance(operand, (pd.Series, np.ndarray)) else (not operand)
            raise UnsafeExpressionError("Unary operator not supported")

        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                # pandas supports &, | for elementwise; but parse uses 'and', which Python AST keeps as BoolOp.
                # We evaluate with elementwise semantics.
                values = [eval_node(v) for v in node.values]
                result = values[0]
                for v in values[1:]:
                    result = result & v
                return result

            if isinstance(node.op, ast.Or):
                values = [eval_node(v) for v in node.values]
                result = values[0]
                for v in values[1:]:
                    result = result | v
                return result

            raise UnsafeExpressionError("Boolean operator not supported")

        if isinstance(node, ast.Compare):
            left = eval_node(node.left)
            result = None
            for op, comparator in zip(node.ops, node.comparators):
                right = eval_node(comparator)
                if isinstance(op, ast.Eq):
                    comp = left == right
                elif isinstance(op, ast.NotEq):
                    comp = left != right
                elif isinstance(op, ast.Lt):
                    comp = left < right
                elif isinstance(op, ast.LtE):
                    comp = left <= right
                elif isinstance(op, ast.Gt):
                    comp = left > right
                elif isinstance(op, ast.GtE):
                    comp = left >= right
                else:
                    raise UnsafeExpressionError("Comparison operator not supported")
                result = comp if result is None else (result & comp)  # chain comparisons conservatively
                left = right
            return result

        raise UnsafeExpressionError(f"Unsupported AST node: {type(node).__name__}")

    return eval_node(tree)


class FormulaBuilder:
    """Build custom formulas for derived columns (safe evaluator; no eval())."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.formulas: Dict[str, Dict[str, Any]] = {}
        self.derived_columns: Dict[str, Dict[str, Any]] = {}

    def _validate_and_parse(self, formula: str) -> ParsedExpression:
        replaced, _cols = _replace_column_tokens_with_identifiers(formula)
        parsed = _parse_expression(replaced)
        _validate_expression_safety(parsed.expr)
        return parsed

    def _eval_arithmetic_or_string(self, formula: str) -> Any:
        replaced, ordered_cols = _replace_column_tokens_with_identifiers(formula)
        parsed = _parse_expression(replaced)
        _validate_expression_safety(parsed.expr)

        variables: Dict[str, Any] = {}
        for i, col in enumerate(ordered_cols):
            variables[f"__col_{i}"] = self.df[col]

        return _safe_eval(parsed.expr, variables)

    def _eval_condition_bool(self, condition_text: str) -> pd.Series:
        replaced, ordered_cols = _replace_column_tokens_with_identifiers(condition_text)
        parsed = _parse_expression(replaced)
        _validate_expression_safety(parsed.expr)

        variables: Dict[str, Any] = {}
        for i, col in enumerate(ordered_cols):
            variables[f"__col_{i}"] = self.df[col]

        result = _safe_eval(parsed.expr, variables)
        if not isinstance(result, (pd.Series, np.ndarray)):
            raise FormulaValidationError("Condition expression did not evaluate to a boolean Series")
        return pd.Series(result, index=self.df.index)

    def _literal_value(self, raw: str) -> Any:
        """
        Parse then-values / constants from user input using ast.literal_eval.
        Allows numbers and strings. If raw isn't a literal, reject.
        """
        try:
            return ast.literal_eval(raw)
        except Exception as e:
            raise FormulaValidationError(f"Then/constant value must be a literal (e.g. 10 or 'A'), got: {raw}") from e

    def create_arithmetic_formula(
        self, name: str, formula: str, description: str = ""
    ) -> Dict[str, Any]:
        try:
            result = self._eval_arithmetic_or_string(formula)

            ref_columns = _extract_column_tokens(formula, self.df)
            self.formulas[name] = {
                "type": "arithmetic",
                "formula": formula,
                "description": description,
                "referenced_columns": ref_columns,
            }

            if hasattr(result, "head"):
                sample_values = result.head(5).tolist()
            else:
                sample_values = [result]

            return {
                "success": True,
                "message": f"Formula '{name}' created successfully",
                "sample_values": sample_values,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_conditional_formula(
        self, name: str, conditions: List[Dict], description: str = ""
    ) -> Dict[str, Any]:
        """
        conditions: list of {"condition": <expr>, "value": <literal>}
        condition expr supports boolean operators and comparisons with [col] references.
        value must be a literal (number or quoted string).
        """
        try:
            if not conditions:
                return {"success": False, "error": "No conditions provided"}

            # Validate all conditions and values
            compiled: List[Tuple[str, Any]] = []
            for c in conditions:
                cond_text = c.get("condition")
                then_raw = c.get("value")
                if not cond_text or then_raw is None:
                    return {"success": False, "error": "Each condition needs 'condition' and 'value'"}
                value = self._literal_value(str(then_raw))
                # Validate condition compiles
                _ = self._eval_condition_bool(str(cond_text))
                compiled.append((str(cond_text), value))

            self.formulas[name] = {
                "type": "conditional",
                "conditions": compiled,
                "description": description,
            }

            return {
                "success": True,
                "message": f"Conditional formula '{name}' created successfully",
                "conditions_count": len(compiled),
            }
        except Exception as e:
            return {"success": False, "error": f"Conditional formula error: {str(e)}"}

    def create_string_formula(
        self, name: str, formula: str, description: str = ""
    ) -> Dict[str, Any]:
        try:
            # Evaluate to validate; also ensure the result is string-like if possible
            result = self._eval_arithmetic_or_string(formula)

            ref_columns = _extract_column_tokens(formula, self.df)
            self.formulas[name] = {
                "type": "string",
                "formula": formula,
                "description": description,
                "referenced_columns": ref_columns,
            }

            sample_values = result.head(5).tolist() if hasattr(result, "head") else [result]
            return {
                "success": True,
                "message": f"String formula '{name}' created successfully",
                "sample_values": sample_values,
            }
        except Exception as e:
            return {"success": False, "error": f"String formula error: {str(e)}"}

    def create_aggregation_formula(
        self,
        name: str,
        column: str,
        agg_type: str,
        group_by: Optional[str] = None,
        description: str = "",
    ) -> Dict[str, Any]:
        try:
            if column not in self.df.columns:
                return {"success": False, "error": f"Column '{column}' not found"}
            if group_by and group_by not in self.df.columns:
                return {"success": False, "error": f"Group column '{group_by}' not found"}

            agg_types = {
                "sum": "sum",
                "mean": "mean",
                "median": "median",
                "min": "min",
                "max": "max",
                "std": "std",
                "count": "count",
                "nunique": "nunique",
            }
            if agg_type not in agg_types:
                return {"success": False, "error": f"Unknown aggregation: {agg_type}"}

            if group_by:
                result = getattr(self.df.groupby(group_by)[column], agg_types[agg_type])()
            else:
                result = getattr(self.df[column], agg_types[agg_type])()

            self.formulas[name] = {
                "type": "aggregation",
                "column": column,
                "agg_type": agg_type,
                "group_by": group_by,
                "description": description,
                "result": result if isinstance(result, (int, float)) else result.to_dict(),
            }

            return {
                "success": True,
                "message": f"Aggregation formula '{name}' created successfully",
                "result": str(result) if isinstance(result, (int, float)) else "Grouped results available",
            }
        except Exception as e:
            return {"success": False, "error": f"Aggregation error: {str(e)}"}

    def apply_formula(self, formula_name: str, target_column: str = None) -> Dict[str, Any]:
        if formula_name not in self.formulas:
            return {"success": False, "error": f"Formula '{formula_name}' not found"}

        try:
            formula_config = self.formulas[formula_name]
            target_col = target_column or f"{formula_name}_result"

            if formula_config["type"] in ("arithmetic", "string"):
                out = self._eval_arithmetic_or_string(formula_config["formula"])
                self.df[target_col] = out

            elif formula_config["type"] == "conditional":
                # Build nested where: cond1 ? v1 : cond2 ? v2 : ... : last_value is v_last (last condition)
                # We interpret each condition with its value; nesting order matches UI order.
                compiled: List[Tuple[str, Any]] = formula_config["conditions"]

                default_value = compiled[-1][1]
                result = pd.Series([default_value] * len(self.df), index=self.df.index)

                # Apply from last-1 backwards
                for cond_text, then_value in reversed(compiled[:-1]):
                    cond = self._eval_condition_bool(cond_text)
                    result = np.where(cond, then_value, result)

                # Ensure Series
                self.df[target_col] = pd.Series(result, index=self.df.index)

            elif formula_config["type"] == "aggregation":
                col = formula_config["column"]
                agg_type = formula_config["agg_type"]
                group_by = formula_config["group_by"]

                if group_by:
                    agg_result = getattr(self.df.groupby(group_by)[col], agg_type)()
                    self.df[target_col] = self.df[group_by].map(agg_result)
                else:
                    self.df[target_col] = getattr(self.df[col], agg_type)()

            else:
                return {"success": False, "error": f"Unsupported formula type: {formula_config['type']}"}

            self.derived_columns[target_col] = formula_config
            return {
                "success": True,
                "message": f"Formula applied to column '{target_col}'",
                "column_name": target_col,
                "sample_values": self.df[target_col].head(5).tolist(),
            }
        except Exception as e:
            return {"success": False, "error": f"Apply formula error: {str(e)}"}

    def list_formulas(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": name,
                "type": config.get("type"),
                "description": config.get("description", ""),
                "referenced_columns": config.get("referenced_columns", []),
            }
            for name, config in self.formulas.items()
        ]

    def delete_formula(self, formula_name: str) -> Dict[str, Any]:
        if formula_name in self.formulas:
            del self.formulas[formula_name]
            return {"success": True, "message": f"Formula '{formula_name}' deleted"}
        return {"success": False, "error": "Formula not found"}

    def get_derived_dataframe(self) -> pd.DataFrame:
        return self.df.copy()

    def export_formulas(self) -> Dict[str, Any]:
        export_data = {
            "formulas": {},
            "derived_columns": list(self.derived_columns.keys()),
            "timestamp": pd.Timestamp.now().isoformat(),
        }

        for name, config in self.formulas.items():
            export_data["formulas"][name] = {k: v for k, v in config.items()}

        return export_data

    def import_formulas(self, formula_dict: Dict[str, Any]) -> Dict[str, Any]:
        try:
            count = 0
            for name, config in formula_dict.get("formulas", {}).items():
                self.formulas[name] = config
                count += 1
            return {"success": True, "message": f"Imported {count} formulas", "formulas_imported": count}
        except Exception as e:
            return {"success": False, "error": str(e)}

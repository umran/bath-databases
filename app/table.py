import sqlite3
from typing import List, Any, Callable, Optional
from enum import Enum
from datetime import datetime

# local imports
from .util import select_int_in_range, select_int_in_range_with_abort, clear_stdout, binary_decision

class DataType(Enum):
    Int = 1
    Text = 2
    Date = 3
    DateTime = 4

class Value:
    type: DataType
    inner: Any

    def __init__(self, type: DataType, inner: Any):
        self.type = type
        self.inner = inner
    
    def to_str(self) -> str:
        match self.type:
            case DataType.Text:
                return self.inner
            case DataType.Int:
                return str(self.inner)
            case DataType.Date:
                return self.inner.strftime('%Y-%m-%d %H:%M:%S')
            case DataType.DateTime:
                return self.inner.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def new_text(val: str) -> 'Value':
        return Value(DataType.Text, val)

    @staticmethod
    def new_int(val: int) -> 'Value':
        return Value(DataType.Int, val)
    
    @staticmethod
    def new_date(val: datetime) -> 'Value':
        return Value(DataType.Date, val)
    
    @staticmethod
    def new_datetime(val: datetime) -> 'Value':
        return Value(DataType.DateTime, val)


class ColumnDef:
    name: str
    type: DataType
    nullable: bool
    allowed_values: Optional[List[Value]]

    def __init__(self, name: str, type: DataType, nullable=False, allowed_values: Optional[List[Value]] = None):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.allowed_values = allowed_values

    def parse_value(self, val: Any) -> Value:
        inner: Any = None

        # only allow null value if column is nullable
        if self.nullable and val is None:
            return Value(self.type, inner)
        elif val is None:
            raise ValueError(f"received null value for non-nullable column with name: {self.name}")

        match self.type:
            case DataType.Int:
                try:
                    inner = int(val)
                except ValueError:
                    raise ValueError(f"parsing integer failed for column with name: {self.name}")
            case DataType.Text:
                try:
                    inner = str(val)
                except ValueError:
                    raise ValueError(f"parsing string failed for column with name: {self.name}")
            case DataType.Date:
                for date_format in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                    try:
                        inner = datetime.strptime(val, date_format)
                        break
                    except ValueError:
                        continue
                
                if inner is None:
                    raise ValueError(f"parsing date failed for column with name: {self.name}")
                # try:
                #     inner = datetime.strptime(val, "%Y-%m-%d")
                # except ValueError:
                #     raise ValueError(f"parsing date failed for column with name: {self.name}")
            case DataType.DateTime:
                try:
                    inner = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    raise ValueError(f"parsing datetime failed for column with name: {self.name}")
        
        return Value(self.type, inner)

class SelectOperator(Enum):
    Eq = 1
    Like = 2
    Gt = 3
    Gte = 4
    Lt = 5
    Lte = 6

    def to_sql_token(self) -> str:
        match self.name:
            case "Like":
                return "LIKE"
            case "Eq":
                return "="
            case "Gt":
                return ">"
            case "Gte":
                return ">="
            case "Lt":
                return "<"
            case "Lte":
                return "<="

class SelectCondition:
    column: ColumnDef
    operator: SelectOperator
    value: Value

    def __init__(self, column: ColumnDef, operator: SelectOperator, value: Value):
        self.column = column
        self.operator = operator
        self.value = value

    # this method is strictly used to display the condition to the user only
    # the output of this method would never be used inside a statement that is executed against the database
    def to_str(self) -> str:
        return f"{self.column.name} {self.operator.name} {self.value.to_str()}"
    
    # this generates a prepared condition that can be used safely inside a statement executed against the db
    def to_prepared_statement(self) -> str:
        return f"{self.column.name} {self.operator.to_sql_token()} ?"

class TableDef:
    name: str
    columns: List[ColumnDef]

    def __init__(self, name, columns: List[ColumnDef]):
        self.name = name
        self.columns = columns

    def read_rows(self, rows: List[sqlite3.Row]) -> List[dict[str, Value]]:
        return [self.read_row(row) for row in rows]

    def read_row(self, row: sqlite3.Row) -> dict[str, Value]:
        parsed_row: dict[str, Value] = {}

        for col_def in self.columns:
            val: Any = None
            
            try:
                val = row[col_def.name]
            except KeyError:
                raise ValueError(f"returned row does not contain a column by the following name: {col_def.name}")

            parsed_row[col_def.name] = col_def.parse_value(val)

        return parsed_row

    def get_column(self) -> ColumnDef:
        clear_stdout()
        
        while True:
            print("Select a column")
            
            for idx, col in enumerate(self.columns):
                print(f"    {idx + 1}. {col.name}")
            
            # this should result in an idx within the correct bounds
            selected_idx = select_int_in_range("Please enter column number: ", 1, len(self.columns)) - 1
            return self.columns[selected_idx]

    def get_operator(self, column: ColumnDef) -> SelectOperator:
        clear_stdout()

        while True:
            print(f"Select an operator for {column.name}")
            
            op_names = SelectOperator._member_names_
            for idx, name in enumerate(op_names):
                print(f"    {idx + 1}. {name}")

            # this should result in an idx within the correct bounds
            selected_idx = select_int_in_range("Please select an operator: ", 1, len(op_names)) - 1
            return SelectOperator.__getitem__(op_names[selected_idx])
    
    def get_value(self, column: ColumnDef, msg: Optional[str] = None) -> Value:
        clear_stdout()

        if msg is None:
            msg = f"Please enter a value for {column.name}: "
        
        while True:
            raw_input = input(msg)
            
            # in the special case that the input is the string "NULL", assume the intent is to set the value to NULL
            if raw_input == "NULL":
                if not column.nullable:
                    print(f"Invalid input. Please enter a valid non-null {column.type.name} value")
                    continue

                raw_input = None

            try:
                val = column.parse_value(raw_input)

                if column.allowed_values is not None:
                    if val.inner not in [allowed_value.inner for allowed_value in column.allowed_values]:
                        print(f"Invalid input. You may only enter one of the following allowed values:")
                        for allowed_value in column.allowed_values:
                            print(f"    {allowed_value.to_str()}")
                        continue

                return val
            except ValueError:
                print(f"Invalid input. Please enter a valid {column.type.name} value")
                continue
    
    def get_select_conditions_optional(self) -> List[SelectCondition]:
        if binary_decision("Would you like to specify some search criteria?"):
            return self.get_select_conditions()
        return []
    
    def get_select_conditions(self) -> List[SelectCondition]:
        clear_stdout()
        conditions: List[SelectCondition] = []
        
        while True:
            column = self.get_column()
            operator = self.get_operator(column)
            value = self.get_value(column)

            clear_stdout()

            conditions.append(SelectCondition(column, operator, value))

            applied_conditions = ", ".join([condition.to_str() for condition in conditions])
            if applied_conditions != "":
                print(f"Applied conditions: {applied_conditions}")

            if binary_decision("Would you like to add any more conditions?"):
                continue
            return conditions
    
    def get_column_values(self, filter: Optional[Callable[[ColumnDef], bool]] = None) -> List[Value]:
        clear_stdout()
        filtered_cols: List[ColumnDef] = []
        
        if filter is None:
            filtered_cols = self.columns
        else:
            for column in self.columns:
                if filter(column):
                    filtered_cols.append(column)

        values: List[Value] = []
        for column in filtered_cols:
            value = self.get_value(column)
            values.append(value)
        
        return values
    
    # this is a generic method that can be used to retrieve records from an equivalent table that
    # exists on the database
    def find_records_from_db(self, cursor: sqlite3.Cursor) -> List[dict[str, Value]]:
        statement = f"""
            SELECT * FROM {self.name}
        """


        conditions = self.get_select_conditions_optional()

        prepared_conditions = [condition.to_prepared_statement() for condition in conditions]
        condition_values = [condition.value.inner for condition in conditions]

        if len(prepared_conditions) > 0:
            where_clause = " AND ".join(prepared_conditions)
            statement = f"{statement} WHERE {where_clause}"

        print(statement)

        results = cursor.execute(statement, condition_values).fetchall()

        return self.read_rows(results)

    def select_record_from_db(self, cursor: sqlite3.Cursor) -> Optional[dict[str, Value]]:
        while True:
            records = self.find_records_from_db(cursor)

            clear_stdout()
            print(f"Your query yielded {len(records)} records")
            for idx, record in enumerate(records):
                display_row = ",    ".join([record[key].to_str() for key in record.keys()])
                print(f"    {idx + 1}. {display_row}")
            
            print(f"    0. Abort")

            maybe_idx = select_int_in_range_with_abort("Please select a record: ", 1, len(records))
            if maybe_idx is None:
                return None

            return records[maybe_idx - 1]

            

def test():
    table = TableDef("flight", [
        ColumnDef("number", DataType.Text),
        ColumnDef("status", DataType.Text, allowed_values=[Value.new_text("scheduled"), Value.new_text("delayed"), Value.new_text("boarding"), Value.new_text("departed"), Value.new_text("arrived")]),
        ColumnDef("date", DataType.Date),
        ColumnDef("scheduled_departure", DataType.DateTime),
        ColumnDef("scheduled_arrival", DataType.DateTime),
    ])

    table.get_column_values(lambda col: col.name != "number")

    # table.get_select_conditions()
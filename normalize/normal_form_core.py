import polars as pl
from itertools import combinations
from more_itertools import flatten
from enum import Enum
from dataclasses import dataclass
from collections import namedtuple
from typing import Optional, Tuple

class ColumnList:

    def __init__(self):
        self.columns = set()
    
    def get_valid(self, col: str):
        if col in self.columns:
            new_id = "_"+str(len([col for col in self.columns if col.startswith(col)]))
            return self.get_valid(col + new_id)
        else:
            self.columns.add(col)
            return col

    def get_valid_id(self, col: str):
        if col in self.columns:
            new_id = "_"+str(len([col for col in self.columns if col.startswith(col)]))+"_id"
            return self.get_valid(col + new_id)
        
        else:
            self.columns.add(col)
            return col
default_column_manager = ColumnList()





def add_index_column(frame: pl.LazyFrame, group: list[str], column_manager: ColumnList) -> Tuple[pl.LazyFrame , str]:
    best_col = column_manager.get_valid_id(list(group)[0])
    compo_name = column_manager.get_valid('composite_index')

    alt_columns = [col +'_str' for col in group if col in frame.columns]
    new_frame = frame.with_columns([pl.col(col).cast(str).suffix('_str') for col in group if col in frame.columns])
    index_frame = new_frame.select(alt_columns).unique().with_row_count(best_col)
    return new_frame.join(index_frame, on=alt_columns, how='inner').drop(alt_columns), best_col



@dataclass
class Relationship:
    on: str
    is_foreign_key: bool

def determine_best_column(frame: pl.LazyFrame, group: set[str]) -> str | None:
    """Factors are (has id or ID or Id in the name), is an int, and is unique"""
    cannidates: list[str] = []
    for col, dtype in frame.schema.items():
        if dtype not in pl.INTEGER_DTYPES:
            continue
        if col not in group:
            continue
    # Unique check is expnesive and already handled by the cardinality check
    if not cannidates:
        return None
    return sorted(cannidates, key=lambda x: x.lower().count("id")**len(x)+len(x))[0]
    

class Table:

    def __init__(self, frame: pl.LazyFrame, relationships: Optional[list[Relationship]] = None):
        self.frame = frame
        self._relationships = relationships
        self._groups: Optional[list[set]] = None # we do not want this to be user specified

    @property
    def relationships(self) -> list[Relationship]:
        if self._relationships is None:
            self._relationships = []
        return self._relationships
    def inherit_rels(self,
        newframe : pl.LazyFrame,
        newrel: Relationship) ->Table:
        new_rels = self.relationships
        new_rels.append(newrel)
        return Table(
        newframe, 
        new_rels)

    @property
    def groups(self) -> list[set[str]]:
        if self._groups is None:
            groups =  determine_groups(self.frame)
            if groups is None:
                return []
            self._groups = groups
        return self._groups

    def __str__(self):
        """ Print data schema and relationships in an easy format"""
        def strline(val):
            return '\n'.join([str(s) for s in val])
        return  (f"Table: Schema:\n{strline(self.frame.schema.items())}"
                f"\nRelationships:\n{strline(self.relationships)}"
                f"\nGroups:\n{strline(self.groups)}")

def decompose_frame(table: Table, group: list[str], column_manager=default_column_manager) -> tuple[Table, Table]:
    #sort by length of groups in descending order
    new_tables: list[Table] = []
    frame = table.frame
    
    
    best_col = determine_best_column(table.frame, group)
    others = list(set([col for col in frame.columns if col not in group] + [best_col]))
    if best_col is None:
        frame, best_col = add_index_column(frame, group, column_manager)
    
    new_group = frame.select(group).unique()
    inherit_rels_new = [rel for rel in table.relationships if rel.on in group] + [Relationship(best_col, is_foreign_key = False)]
    inherit_rels_old = [rel for rel in table.relationships if rel.on not in group] + [Relationship(best_col, is_foreign_key = True)]
    left_table = Table(frame.select(group), inherit_rels_new)
    right_table = Table(frame.select(others), inherit_rels_old)
    return left_table, right_table

    best_col = column_manager.get_valid_id(group[0])
    
    compo_index = ''
    for col in group:
        compo_index = compo_index + pl.col(col).cast(str)
    
    new_group_frame = frame.select(group).unique().with_row_count(best_col)
    
def determine_groups(frame: pl.LazyFrame) -> list[set[str]]:
    current_groups: dict[str, set[str]] = {}
    for a, b in combinations(frame.columns, 2):
        current_cols = {a,b}
        if not determine_cardinality(a, b, frame.select(current_cols).collect()):
            continue
        for key, group in current_groups.items():
            if current_cols.intersection(group):
                group.update(current_cols)
                current_groups[key] = group
                break
        else:
            current_groups[a] = current_cols
    return list(current_groups.values())

def determine_cardinality(col1: str, col2 : str, table: pl.DataFrame) -> bool:
    if not {col1, col2}.issubset(table.columns):
        raise ValueError(f"column {col1} or {col2} not found in the table that has {table.columns}")
    print(col1,col2, table)
    test_frame = table.select([pl.col(col).cast(str) for col in (col1,col2)]).unique(subset=[col1,col2]).with_row_count(f"{col1}_{col2}")
    try:
        col1_height =  test_frame.select(col1).unique().height
        col2_height =  test_frame.select(col2).unique().height
    except pl.ColumnNotFoundError as e:
        print(e)
    return col1_height == col2_height and col2_height == test_frame.height


def fully_define_groups(table: Table) -> list[Table]:
    new_tables = []
    for group in table.groups:
        if len(group) <3:
            continue
        new_items = decompose_frame(table, group)
        new_tables.extend(new_items)
        for table in new_items:
            print(table)
    return new_tables

def lazy_frame_to_model(frame: pl.LazyFrame) -> list[Table]:
    strings = extract_strings(Table(frame))
    return fully_define_groups(strings[-1]) + strings[:-1]

def extract_strings(table: Table) -> list[Table]:
    new_tables = []
    for col, dtype in table.frame.schema.items():
        if 'utf' in str(dtype).lower() or dtype == pl.Categorical:
            table, right_table = decompose_frame(table, {col})
            new_tables.append(right_table)
    new_tables.append(table)
    return new_tables
def fully_define_groups_test_floats_ints_bools():

    test_data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "age": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        "height": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 1.5, 8.8, 9.9],
        "weight": [100, 200, 300, 400, 500, 600, 700, 800, 900],
        "waist": [10, 20, 30, 40, 50, 60, 70, 80, 90]})
        
    table = Table(test_data)
    new_tables = fully_define_groups(table)
    print(len(new_tables))



def fully_define_groups_test():
    test_data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "age": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        "height": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 1.5, 8.8, 9.9],
        "weight": [100, 200, 300, 400, 500, 600, 700, 800, 900],
        "waist": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        "motivation": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
        "range": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "is_active": [True, False, True, False, True, False, True, False, True],
        "is_active2": [None, False, True, False, True, False, True, False, True],
        "is_active3": [True, False, True, False, True, False, True, False, True],
        "is_active4": [True, False, True, False, True, False, True, False, True],
        "is_active5": [True, False, True, False, True, False, True, False, True],
        "workstation": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
        "model": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
        "vals": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
    new_tables = lazy_frame_to_model(test_data.lazy())
    for table in new_tables:
        print(table)

fully_define_groups_test()